package authn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	sessionScanCount    = 100
	maxRedisDeleteBatch = 100
)

var (
	ErrSessionNotFound      = errors.New("session not found")
	ErrSessionUserMismatch  = errors.New("session user mismatch")
	ErrRefreshTokenMismatch = errors.New("refresh token mismatch")
)

type Session struct {
	ID                       string    `json:"id"`
	UserID                   int64     `json:"userId"`
	RefreshTokenHash         string    `json:"refreshTokenHash"`
	CreatedAt                time.Time `json:"createdAt"`
	ExpiresAt                time.Time `json:"expiresAt"`
	PreviousRefreshTokenHash string    `json:"previousRefreshTokenHash,omitempty"`
	RefreshReplayUntil       int64     `json:"refreshReplayUntil,omitempty"`
	EncryptedRefreshResult   string    `json:"encryptedRefreshResult,omitempty"`
}

type SessionReader interface {
	Get(ctx context.Context, sessionID string) (*Session, error)
}

type SessionStore interface {
	SessionReader
	Create(ctx context.Context, session Session, ttl time.Duration) error
	RotateRefreshToken(
		ctx context.Context,
		sessionID string,
		currentHash string,
		nextHash string,
		expiresAt time.Time,
		ttl time.Duration,
		encryptedResult string,
	) (*Session, error)
	Revoke(ctx context.Context, userID int64, sessionID string) error
	RevokeAll(ctx context.Context, userID int64) error
}

type redisSessionReadClient interface {
	GetCtx(ctx context.Context, key string) (string, error)
}

type redisSessionClient interface {
	redisSessionReadClient
	SetexCtx(ctx context.Context, key, value string, seconds int) error
	DelCtx(ctx context.Context, keys ...string) (int, error)
	SaddCtx(ctx context.Context, key string, values ...any) (int, error)
	SremCtx(ctx context.Context, key string, values ...any) (int, error)
	SscanCtx(ctx context.Context, key string, cursor uint64, match string, count int64) ([]string, uint64, error)
	ExpireCtx(ctx context.Context, key string, seconds int) error
	EvalCtx(ctx context.Context, script string, keys []string, args ...any) (any, error)
}

type RedisSessionReader struct {
	client        redisSessionReadClient
	sessionPrefix string
}

type RedisSessionStore struct {
	*RedisSessionReader
	client             redisSessionClient
	userSessionsPrefix string
}

func NewRedisSessionReader(client redisSessionReadClient, sessionPrefix string) (*RedisSessionReader, error) {
	if client == nil {
		return nil, errors.New("redis session read client is nil")
	}

	sessionPrefix = normalizeKeyPrefix(sessionPrefix)
	if sessionPrefix == "" {
		return nil, errors.New("session key prefix is empty")
	}

	return &RedisSessionReader{client: client, sessionPrefix: sessionPrefix}, nil
}

func NewRedisSessionStore(
	client redisSessionClient,
	sessionPrefix string,
	userSessionsPrefix string,
) (*RedisSessionStore, error) {
	reader, err := NewRedisSessionReader(client, sessionPrefix)
	if err != nil {
		return nil, err
	}
	userSessionsPrefix = normalizeKeyPrefix(userSessionsPrefix)
	if userSessionsPrefix == "" {
		return nil, errors.New("user sessions key prefix is empty")
	}

	return &RedisSessionStore{
		RedisSessionReader: reader,
		client:             client,
		userSessionsPrefix: userSessionsPrefix,
	}, nil
}

func (s *RedisSessionStore) Create(ctx context.Context, session Session, ttl time.Duration) error {
	if err := validateSession(session); err != nil {
		return err
	}
	seconds, err := ttlSeconds(ttl)
	if err != nil {
		return err
	}

	value, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("encode session: %w", err)
	}

	sessionKey := s.sessionKey(session.ID)
	userKey := s.userSessionsKey(session.UserID)
	// The session value is authoritative; the per-user set is only the RevokeAll
	// index. Compensate if index construction fails so no live session becomes
	// undiscoverable by bulk revocation.
	if err := s.client.SetexCtx(ctx, sessionKey, string(value), seconds); err != nil {
		return fmt.Errorf("store session: %w", err)
	}
	if _, err := s.client.SaddCtx(ctx, userKey, session.ID); err != nil {
		_, _ = s.client.DelCtx(ctx, sessionKey)
		return fmt.Errorf("index user session: %w", err)
	}
	if err := s.client.ExpireCtx(ctx, userKey, seconds); err != nil {
		_, _ = s.client.DelCtx(ctx, sessionKey)
		_, _ = s.client.SremCtx(ctx, userKey, session.ID)
		return fmt.Errorf("expire user session index: %w", err)
	}

	return nil
}

func (s *RedisSessionReader) Get(ctx context.Context, sessionID string) (*Session, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, ErrSessionNotFound
	}

	value, err := s.client.GetCtx(ctx, s.sessionKey(sessionID))
	if err != nil {
		return nil, fmt.Errorf("load session: %w", err)
	}
	if value == "" {
		return nil, ErrSessionNotFound
	}

	var session Session
	if err := json.Unmarshal([]byte(value), &session); err != nil {
		return nil, fmt.Errorf("decode session: %w", err)
	}
	if err := validateSession(session); err != nil {
		return nil, fmt.Errorf("decode session: %w", err)
	}
	if session.ID != sessionID {
		return nil, errors.New("stored session id does not match its key")
	}

	return &session, nil
}

// RotateRefreshToken uses Redis time and a single-key atomic decision. A Go-side
// hash check could revoke a session using a snapshot from before another refresh.
func (s *RedisSessionStore) RotateRefreshToken(
	ctx context.Context,
	sessionID string,
	currentHash string,
	nextHash string,
	expiresAt time.Time,
	ttl time.Duration,
	encryptedResult string,
) (*Session, error) {
	if strings.TrimSpace(sessionID) == "" || currentHash == "" || nextHash == "" || expiresAt.IsZero() || encryptedResult == "" {
		return nil, errors.New("invalid refresh token rotation")
	}
	seconds, err := ttlSeconds(ttl)
	if err != nil {
		return nil, err
	}

	session, err := s.Get(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	userKey := s.userSessionsKey(session.UserID)
	if _, err := s.client.SaddCtx(ctx, userKey, sessionID); err != nil {
		return nil, fmt.Errorf("index rotated session: %w", err)
	}
	if err := s.client.ExpireCtx(ctx, userKey, seconds); err != nil {
		return nil, fmt.Errorf("extend user session index: %w", err)
	}

	// Success returns the winning stored JSON, including on retries. Returning our
	// locally generated candidate here would give concurrent callers invalid tokens.
	result, err := s.client.EvalCtx(
		ctx,
		rotateRefreshTokenScript,
		[]string{s.sessionKey(sessionID)},
		currentHash,
		nextHash,
		expiresAt.UTC().Format(time.RFC3339Nano),
		seconds,
		encryptedResult,
	)
	if err != nil {
		return nil, fmt.Errorf("rotate refresh token: %w", err)
	}

	if value, ok := result.(string); ok && strings.HasPrefix(value, "{") {
		if err := json.Unmarshal([]byte(value), session); err != nil {
			return nil, fmt.Errorf("decode rotated session: %w", err)
		}
		if err := validateSession(*session); err != nil || session.ID != sessionID {
			return nil, errors.New("invalid rotated session")
		}
		return session, nil
	}
	switch scriptInteger(result) {
	case -1:
		_, _ = s.client.SremCtx(ctx, userKey, sessionID)
		return nil, ErrRefreshTokenMismatch
	default:
		_, _ = s.client.SremCtx(ctx, userKey, sessionID)
		return nil, ErrSessionNotFound
	}
}

func (s *RedisSessionStore) Revoke(ctx context.Context, userID int64, sessionID string) error {
	if userID <= 0 || strings.TrimSpace(sessionID) == "" {
		return errors.New("invalid session revocation")
	}

	session, err := s.Get(ctx, sessionID)
	if err != nil && !errors.Is(err, ErrSessionNotFound) {
		return err
	}
	if err == nil && session.UserID != userID {
		return ErrSessionUserMismatch
	}

	if _, err := s.client.DelCtx(ctx, s.sessionKey(sessionID)); err != nil {
		return fmt.Errorf("delete session: %w", err)
	}
	if _, err := s.client.SremCtx(ctx, s.userSessionsKey(userID), sessionID); err != nil {
		return fmt.Errorf("remove user session index: %w", err)
	}

	return nil
}

// RevokeAll traverses the explicit per-user index with SSCAN and deletes
// session keys in bounded batches. Do not replace this with Redis KEYS, which
// scans and blocks the complete keyspace.
func (s *RedisSessionStore) RevokeAll(ctx context.Context, userID int64) error {
	if userID <= 0 {
		return errors.New("invalid user session revocation")
	}

	userKey := s.userSessionsKey(userID)
	var cursor uint64
	for {
		sessionIDs, nextCursor, err := s.client.SscanCtx(ctx, userKey, cursor, "*", sessionScanCount)
		if err != nil {
			return fmt.Errorf("scan user sessions: %w", err)
		}
		if err := s.deleteSessionBatch(ctx, sessionIDs); err != nil {
			return err
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	if _, err := s.client.DelCtx(ctx, userKey); err != nil {
		return fmt.Errorf("delete user session index: %w", err)
	}
	return nil
}

func (s *RedisSessionStore) deleteSessionBatch(ctx context.Context, sessionIDs []string) error {
	for offset := 0; offset < len(sessionIDs); offset += maxRedisDeleteBatch {
		end := offset + maxRedisDeleteBatch
		if end > len(sessionIDs) {
			end = len(sessionIDs)
		}

		keys := make([]string, 0, end-offset)
		for _, sessionID := range sessionIDs[offset:end] {
			if sessionID != "" {
				keys = append(keys, s.sessionKey(sessionID))
			}
		}
		if len(keys) == 0 {
			continue
		}
		if _, err := s.client.DelCtx(ctx, keys...); err != nil {
			return fmt.Errorf("delete user sessions: %w", err)
		}
	}
	return nil
}

func (s *RedisSessionReader) sessionKey(sessionID string) string {
	return s.sessionPrefix + ":" + sessionID
}

func (s *RedisSessionStore) userSessionsKey(userID int64) string {
	return s.userSessionsPrefix + ":" + strconv.FormatInt(userID, 10)
}

func normalizeKeyPrefix(prefix string) string {
	return strings.TrimSuffix(strings.TrimSpace(prefix), ":")
}

func validateSession(session Session) error {
	if session.ID == "" || session.UserID <= 0 || session.RefreshTokenHash == "" ||
		session.CreatedAt.IsZero() || session.ExpiresAt.IsZero() || !session.ExpiresAt.After(session.CreatedAt) {
		return errors.New("invalid session")
	}
	return nil
}

func ttlSeconds(ttl time.Duration) (int, error) {
	if ttl <= 0 {
		return 0, errors.New("session ttl must be positive")
	}
	seconds := math.Ceil(ttl.Seconds())
	if seconds > float64(math.MaxInt) {
		return 0, errors.New("session ttl is too large")
	}
	return int(seconds), nil
}

func scriptInteger(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case string:
		parsed, _ := strconv.ParseInt(typed, 10, 64)
		return parsed
	default:
		return 0
	}
}

// KEYS[1] below is Redis Lua's declared-key parameter, not the Redis KEYS command.
const rotateRefreshTokenScript = `
local value = redis.call('GET', KEYS[1])
if not value then
    return 0
end

local session = cjson.decode(value)
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local withinWindow = session.refreshReplayUntil and now < session.refreshReplayUntil
if withinWindow and session.encryptedRefreshResult and
    (session.refreshTokenHash == ARGV[1] or session.previousRefreshTokenHash == ARGV[1]) then
    return value
end
if session.refreshTokenHash ~= ARGV[1] then
    redis.call('DEL', KEYS[1])
    return -1
end

session.previousRefreshTokenHash = session.refreshTokenHash
session.refreshTokenHash = ARGV[2]
session.expiresAt = ARGV[3]
session.refreshReplayUntil = now + 5000
session.encryptedRefreshResult = ARGV[5]
local updated = cjson.encode(session)
redis.call('SETEX', KEYS[1], tonumber(ARGV[4]), updated)
return updated
`
