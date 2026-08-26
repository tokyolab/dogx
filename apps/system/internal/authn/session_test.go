package authn

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"
)

type redisSessionClientStub struct {
	values map[string]string
	sets   map[string]map[string]struct{}
	ttls   map[string]int

	setexErr   error
	getErr     error
	delErr     error
	saddErr    error
	sremErr    error
	scanErr    error
	expireErr  error
	evalErr    error
	evalResult *int64
}

func newRedisSessionClientStub() *redisSessionClientStub {
	return &redisSessionClientStub{
		values: make(map[string]string),
		sets:   make(map[string]map[string]struct{}),
		ttls:   make(map[string]int),
	}
}

func (s *redisSessionClientStub) SetexCtx(_ context.Context, key, value string, seconds int) error {
	if s.setexErr != nil {
		return s.setexErr
	}
	s.values[key] = value
	s.ttls[key] = seconds
	return nil
}

func (s *redisSessionClientStub) GetCtx(_ context.Context, key string) (string, error) {
	if s.getErr != nil {
		return "", s.getErr
	}
	return s.values[key], nil
}

func (s *redisSessionClientStub) DelCtx(_ context.Context, keys ...string) (int, error) {
	if s.delErr != nil {
		return 0, s.delErr
	}
	deleted := 0
	for _, key := range keys {
		if _, ok := s.values[key]; ok {
			delete(s.values, key)
			deleted++
		}
		if _, ok := s.sets[key]; ok {
			delete(s.sets, key)
			deleted++
		}
		delete(s.ttls, key)
	}
	return deleted, nil
}

func (s *redisSessionClientStub) SaddCtx(_ context.Context, key string, values ...any) (int, error) {
	if s.saddErr != nil {
		return 0, s.saddErr
	}
	if s.sets[key] == nil {
		s.sets[key] = make(map[string]struct{})
	}
	added := 0
	for _, value := range values {
		member := value.(string)
		if _, ok := s.sets[key][member]; !ok {
			s.sets[key][member] = struct{}{}
			added++
		}
	}
	return added, nil
}

func (s *redisSessionClientStub) SremCtx(_ context.Context, key string, values ...any) (int, error) {
	if s.sremErr != nil {
		return 0, s.sremErr
	}
	removed := 0
	for _, value := range values {
		member := value.(string)
		if _, ok := s.sets[key][member]; ok {
			delete(s.sets[key], member)
			removed++
		}
	}
	return removed, nil
}

func (s *redisSessionClientStub) SscanCtx(
	_ context.Context,
	key string,
	cursor uint64,
	_ string,
	count int64,
) ([]string, uint64, error) {
	if s.scanErr != nil {
		return nil, 0, s.scanErr
	}
	members := make([]string, 0, len(s.sets[key]))
	for member := range s.sets[key] {
		members = append(members, member)
	}
	sort.Strings(members)
	start := int(cursor)
	if start >= len(members) {
		return nil, 0, nil
	}
	end := start + int(count)
	if end >= len(members) {
		return members[start:], 0, nil
	}
	return members[start:end], uint64(end), nil
}

func (s *redisSessionClientStub) ExpireCtx(_ context.Context, key string, seconds int) error {
	if s.expireErr != nil {
		return s.expireErr
	}
	s.ttls[key] = seconds
	return nil
}

func (s *redisSessionClientStub) EvalCtx(
	_ context.Context,
	_ string,
	keys []string,
	args ...any,
) (any, error) {
	if s.evalErr != nil {
		return nil, s.evalErr
	}
	if s.evalResult != nil {
		result := *s.evalResult
		if result <= 0 {
			delete(s.values, keys[0])
		}
		return result, nil
	}
	value := s.values[keys[0]]
	if value == "" {
		return int64(0), nil
	}
	var session Session
	if err := json.Unmarshal([]byte(value), &session); err != nil {
		return nil, err
	}
	if session.RefreshTokenHash != args[0].(string) {
		delete(s.values, keys[0])
		return int64(-1), nil
	}
	session.RefreshTokenHash = args[1].(string)
	expiresAt, err := time.Parse(time.RFC3339Nano, args[2].(string))
	if err != nil {
		return nil, err
	}
	session.ExpiresAt = expiresAt
	encoded, _ := json.Marshal(session)
	s.values[keys[0]] = string(encoded)
	s.ttls[keys[0]] = args[3].(int)
	return int64(1), nil
}

func TestRedisSessionStoreCreateAndGet(t *testing.T) {
	client := newRedisSessionClientStub()
	store, err := NewRedisSessionStore(client, " dogx:test:session: ", " dogx:test:user_sessions: ")
	if err != nil {
		t.Fatalf("create session store: %v", err)
	}

	session := validSession("session-id", 42)
	if err := store.Create(context.Background(), session, 1500*time.Millisecond); err != nil {
		t.Fatalf("store session: %v", err)
	}

	if client.ttls["dogx:test:session:session-id"] != 2 ||
		client.ttls["dogx:test:user_sessions:42"] != 2 {
		t.Fatalf("unexpected Redis TTLs: %+v", client.ttls)
	}
	if _, ok := client.sets["dogx:test:user_sessions:42"]["session-id"]; !ok {
		t.Fatal("session was not added to the user index")
	}

	loaded, err := store.Get(context.Background(), session.ID)
	if err != nil {
		t.Fatalf("load session: %v", err)
	}
	if *loaded != session {
		t.Fatalf("unexpected stored session: %+v", loaded)
	}
}

func TestRedisSessionStoreRotatesRefreshTokenAndRevokesReuse(t *testing.T) {
	client := newRedisSessionClientStub()
	store, _ := NewRedisSessionStore(client, "session", "user-sessions")
	session := validSession("session-id", 42)
	if err := store.Create(context.Background(), session, time.Hour); err != nil {
		t.Fatalf("create session: %v", err)
	}

	nextExpiry := session.ExpiresAt.Add(time.Hour)
	rotated, err := store.RotateRefreshToken(
		context.Background(),
		session.ID,
		session.RefreshTokenHash,
		"next-hash",
		nextExpiry,
		2*time.Hour,
	)
	if err != nil {
		t.Fatalf("rotate refresh token: %v", err)
	}
	if rotated.RefreshTokenHash != "next-hash" || !rotated.ExpiresAt.Equal(nextExpiry) {
		t.Fatalf("unexpected rotated session: %+v", rotated)
	}

	_, err = store.RotateRefreshToken(
		context.Background(),
		session.ID,
		session.RefreshTokenHash,
		"another-hash",
		nextExpiry.Add(time.Hour),
		3*time.Hour,
	)
	if !errors.Is(err, ErrRefreshTokenMismatch) {
		t.Fatalf("expected refresh token reuse to be rejected, got: %v", err)
	}
	if _, err := store.Get(context.Background(), session.ID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("reused refresh token did not revoke session: %v", err)
	}
}

func TestRedisSessionStoreRevokeAndRevokeAll(t *testing.T) {
	client := newRedisSessionClientStub()
	store, _ := NewRedisSessionStore(client, "session", "user-sessions")
	for index := 0; index < 205; index++ {
		session := validSession("session-"+string(rune(index+1000)), 42)
		if err := store.Create(context.Background(), session, time.Hour); err != nil {
			t.Fatalf("create session %d: %v", index, err)
		}
	}
	other := validSession("other-session", 7)
	if err := store.Create(context.Background(), other, time.Hour); err != nil {
		t.Fatalf("create other user session: %v", err)
	}

	if err := store.Revoke(context.Background(), 7, other.ID); err != nil {
		t.Fatalf("revoke session: %v", err)
	}
	if err := store.RevokeAll(context.Background(), 42); err != nil {
		t.Fatalf("revoke all sessions: %v", err)
	}
	if len(client.sets["user-sessions:42"]) != 0 {
		t.Fatal("user session index was not deleted")
	}
	for key := range client.values {
		if key != "" {
			t.Fatalf("session key survived revocation: %s", key)
		}
	}
}

func TestRedisSessionStoreRejectsInvalidInputAndPropagatesErrors(t *testing.T) {
	client := newRedisSessionClientStub()
	if _, err := NewRedisSessionStore(nil, "session", "users"); err == nil {
		t.Fatal("expected nil Redis client to be rejected")
	}
	if _, err := NewRedisSessionStore(client, " ", "users"); err == nil {
		t.Fatal("expected empty session prefix to be rejected")
	}
	if _, err := NewRedisSessionStore(client, "session", " "); err == nil {
		t.Fatal("expected empty user sessions prefix to be rejected")
	}

	store, _ := NewRedisSessionStore(client, "session", "users")
	if err := store.Create(context.Background(), Session{}, time.Minute); err == nil {
		t.Fatal("expected invalid session to be rejected")
	}
	if err := store.Create(context.Background(), validSession("id", 1), 0); err == nil {
		t.Fatal("expected invalid ttl to be rejected")
	}
	if _, err := store.Get(context.Background(), "missing"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected missing session error, got: %v", err)
	}

	redisErr := errors.New("redis unavailable")
	client.setexErr = redisErr
	if err := store.Create(context.Background(), validSession("id", 1), time.Minute); !errors.Is(err, redisErr) {
		t.Fatalf("expected Redis error, got: %v", err)
	}
}

func TestRedisSessionStoreCreateRollsBackPartialWrites(t *testing.T) {
	redisErr := errors.New("redis unavailable")
	tests := []struct {
		name      string
		configure func(*redisSessionClientStub)
	}{
		{
			name: "user index write fails",
			configure: func(client *redisSessionClientStub) {
				client.saddErr = redisErr
			},
		},
		{
			name: "user index expiry fails",
			configure: func(client *redisSessionClientStub) {
				client.expireErr = redisErr
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newRedisSessionClientStub()
			test.configure(client)
			store, _ := NewRedisSessionStore(client, "session", "users")

			err := store.Create(context.Background(), validSession("session-id", 42), time.Hour)
			if !errors.Is(err, redisErr) {
				t.Fatalf("create error = %v, want Redis failure", err)
			}
			if _, exists := client.values["session:session-id"]; exists {
				t.Fatal("partially created session was not removed")
			}
			if _, exists := client.sets["users:42"]["session-id"]; exists {
				t.Fatal("partially created user session index was not removed")
			}
		})
	}
}

func TestRedisSessionReaderRejectsCorruptOrMismatchedData(t *testing.T) {
	client := newRedisSessionClientStub()
	store, _ := NewRedisSessionStore(client, "session", "users")
	client.values["session:session-id"] = "not-json"

	if _, err := store.Get(context.Background(), "session-id"); err == nil ||
		!strings.Contains(err.Error(), "decode session") {
		t.Fatalf("corrupt stored session error = %v", err)
	}

	mismatched := validSession("different-id", 42)
	encoded, err := json.Marshal(mismatched)
	if err != nil {
		t.Fatalf("encode mismatched session: %v", err)
	}
	client.values["session:session-id"] = string(encoded)
	if _, err := store.Get(context.Background(), "session-id"); err == nil ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatched stored session error = %v", err)
	}
}

func TestRedisSessionStoreRotationCleansIndexAfterAtomicRace(t *testing.T) {
	tests := []struct {
		name   string
		result int64
		want   error
	}{
		{name: "refresh hash changed concurrently", result: -1, want: ErrRefreshTokenMismatch},
		{name: "session disappeared concurrently", result: 0, want: ErrSessionNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newRedisSessionClientStub()
			client.evalResult = &test.result
			store, _ := NewRedisSessionStore(client, "session", "users")
			session := validSession("session-id", 42)
			if err := store.Create(context.Background(), session, time.Hour); err != nil {
				t.Fatalf("create session: %v", err)
			}

			_, err := store.RotateRefreshToken(
				context.Background(),
				session.ID,
				session.RefreshTokenHash,
				"next-hash",
				session.ExpiresAt.Add(time.Hour),
				2*time.Hour,
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("rotation error = %v, want %v", err, test.want)
			}
			if _, exists := client.sets["users:42"][session.ID]; exists {
				t.Fatal("failed atomic rotation left a stale user session index")
			}
		})
	}
}

func TestRedisSessionStorePropagatesRotationAndRevocationFailures(t *testing.T) {
	redisErr := errors.New("redis unavailable")

	t.Run("atomic rotation failure", func(t *testing.T) {
		client := newRedisSessionClientStub()
		store, _ := NewRedisSessionStore(client, "session", "users")
		session := validSession("session-id", 42)
		if err := store.Create(context.Background(), session, time.Hour); err != nil {
			t.Fatalf("create session: %v", err)
		}
		client.evalErr = redisErr
		_, err := store.RotateRefreshToken(
			context.Background(),
			session.ID,
			session.RefreshTokenHash,
			"next-hash",
			session.ExpiresAt.Add(time.Hour),
			2*time.Hour,
		)
		if !errors.Is(err, redisErr) {
			t.Fatalf("rotation error = %v, want Redis failure", err)
		}
	})

	t.Run("refresh reuse revocation failure", func(t *testing.T) {
		client := newRedisSessionClientStub()
		store, _ := NewRedisSessionStore(client, "session", "users")
		session := validSession("session-id", 42)
		if err := store.Create(context.Background(), session, time.Hour); err != nil {
			t.Fatalf("create session: %v", err)
		}
		client.delErr = redisErr
		_, err := store.RotateRefreshToken(
			context.Background(),
			session.ID,
			"reused-old-hash",
			"next-hash",
			session.ExpiresAt.Add(time.Hour),
			2*time.Hour,
		)
		if !errors.Is(err, redisErr) || !strings.Contains(err.Error(), "revoke session") {
			t.Fatalf("reuse revocation error = %v, want Redis failure", err)
		}
	})

	t.Run("session ownership mismatch", func(t *testing.T) {
		client := newRedisSessionClientStub()
		store, _ := NewRedisSessionStore(client, "session", "users")
		session := validSession("session-id", 42)
		if err := store.Create(context.Background(), session, time.Hour); err != nil {
			t.Fatalf("create session: %v", err)
		}
		if err := store.Revoke(context.Background(), 7, session.ID); !errors.Is(err, ErrSessionUserMismatch) {
			t.Fatalf("ownership error = %v, want %v", err, ErrSessionUserMismatch)
		}
		if _, exists := client.values["session:"+session.ID]; !exists {
			t.Fatal("ownership mismatch deleted another user's session")
		}
	})
}

func TestRedisSessionStoreRevokeAllPropagatesScanAndDeleteFailures(t *testing.T) {
	redisErr := errors.New("redis unavailable")
	tests := []struct {
		name      string
		configure func(*redisSessionClientStub)
		seed      bool
	}{
		{
			name: "scan fails",
			configure: func(client *redisSessionClientStub) {
				client.scanErr = redisErr
			},
		},
		{
			name: "session batch delete fails",
			seed: true,
			configure: func(client *redisSessionClientStub) {
				client.delErr = redisErr
			},
		},
		{
			name: "user index delete fails",
			configure: func(client *redisSessionClientStub) {
				client.delErr = redisErr
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newRedisSessionClientStub()
			store, _ := NewRedisSessionStore(client, "session", "users")
			if test.seed {
				if err := store.Create(context.Background(), validSession("session-id", 42), time.Hour); err != nil {
					t.Fatalf("create session: %v", err)
				}
			}
			test.configure(client)
			if err := store.RevokeAll(context.Background(), 42); !errors.Is(err, redisErr) {
				t.Fatalf("revoke-all error = %v, want Redis failure", err)
			}
		})
	}
}

func validSession(id string, userID int64) Session {
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	return Session{
		ID:               id,
		UserID:           userID,
		RefreshTokenHash: "refresh-hash",
		CreatedAt:        now,
		ExpiresAt:        now.Add(time.Hour),
	}
}
