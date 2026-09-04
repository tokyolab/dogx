package authn

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

const (
	sessionIDBytes     = 16
	refreshSecretBytes = 32
	tokenIDBytes       = 16
)

var ErrInvalidRefreshToken = errors.New("invalid refresh token")

type TokenConfig struct {
	AccessSecret  string
	AccessExpire  time.Duration
	RefreshExpire time.Duration
	Issuer        string
}

type Credentials struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int64
}

type CredentialIssuer interface {
	Issue(ctx context.Context, userID int64) (*Credentials, error)
}

type CredentialRefresher interface {
	Refresh(ctx context.Context, refreshToken string) (*Credentials, error)
}

type RoleProvider interface {
	ListEnabledRoleIDs(ctx context.Context, userID int64) ([]int64, error)
	IsSuperAdmin(ctx context.Context, userID int64) (bool, error)
}

type TokenIssuer struct {
	config   TokenConfig
	sessions SessionStore
	roles    RoleProvider
	now      func() time.Time
	random   io.Reader
}

func NewTokenIssuer(config TokenConfig, sessions SessionStore, roles RoleProvider) (*TokenIssuer, error) {
	if len(config.AccessSecret) < 32 {
		return nil, errors.New("access token secret must contain at least 32 bytes")
	}
	if config.AccessExpire <= 0 {
		return nil, errors.New("access token expiry must be positive")
	}
	if config.RefreshExpire <= config.AccessExpire {
		return nil, errors.New("refresh token expiry must be greater than access token expiry")
	}
	if config.Issuer == "" {
		return nil, errors.New("access token issuer is empty")
	}
	if sessions == nil {
		return nil, errors.New("session store is nil")
	}
	if roles == nil {
		return nil, errors.New("role provider is nil")
	}

	return &TokenIssuer{
		config:   config,
		sessions: sessions,
		roles:    roles,
		now:      time.Now,
		random:   rand.Reader,
	}, nil
}

func (i *TokenIssuer) Issue(ctx context.Context, userID int64) (*Credentials, error) {
	if userID <= 0 {
		return nil, errors.New("user id must be positive")
	}

	roleIDs, err := i.roleIDs(ctx, userID)
	if err != nil {
		return nil, err
	}
	isSuperAdmin, err := i.isSuperAdmin(ctx, userID)
	if err != nil {
		return nil, err
	}
	sessionID, err := randomString(i.random, sessionIDBytes)
	if err != nil {
		return nil, fmt.Errorf("generate session id: %w", err)
	}
	refreshSecret, err := randomString(i.random, refreshSecretBytes)
	if err != nil {
		return nil, fmt.Errorf("generate refresh token: %w", err)
	}

	now := i.now().UTC()
	accessToken, err := i.signAccessToken(userID, sessionID, roleIDs, isSuperAdmin, now)
	if err != nil {
		return nil, err
	}

	session := Session{
		ID:               sessionID,
		UserID:           userID,
		RefreshTokenHash: hashRefreshSecret(refreshSecret),
		CreatedAt:        now,
		ExpiresAt:        now.Add(i.config.RefreshExpire),
	}
	if err := i.sessions.Create(ctx, session, i.config.RefreshExpire); err != nil {
		return nil, err
	}

	return i.credentials(accessToken, sessionID, refreshSecret), nil
}

func (i *TokenIssuer) Refresh(ctx context.Context, refreshToken string) (*Credentials, error) {
	sessionID, currentSecret, err := parseRefreshToken(refreshToken)
	if err != nil {
		return nil, ErrInvalidRefreshToken
	}

	session, err := i.sessions.Get(ctx, sessionID)
	if errors.Is(err, ErrSessionNotFound) {
		return nil, ErrInvalidRefreshToken
	}
	if err != nil {
		return nil, err
	}

	roleIDs, err := i.roleIDs(ctx, session.UserID)
	if err != nil {
		return nil, err
	}
	isSuperAdmin, err := i.isSuperAdmin(ctx, session.UserID)
	if err != nil {
		return nil, err
	}
	nextSecret, err := randomString(i.random, refreshSecretBytes)
	if err != nil {
		return nil, fmt.Errorf("generate refresh token: %w", err)
	}
	now := i.now().UTC()
	accessToken, err := i.signAccessToken(session.UserID, sessionID, roleIDs, isSuperAdmin, now)
	if err != nil {
		return nil, err
	}

	_, err = i.sessions.RotateRefreshToken(
		ctx,
		sessionID,
		hashRefreshSecret(currentSecret),
		hashRefreshSecret(nextSecret),
		now.Add(i.config.RefreshExpire),
		i.config.RefreshExpire,
	)
	if errors.Is(err, ErrSessionNotFound) || errors.Is(err, ErrRefreshTokenMismatch) {
		return nil, ErrInvalidRefreshToken
	}
	if err != nil {
		return nil, err
	}

	return i.credentials(accessToken, sessionID, nextSecret), nil
}

func (i *TokenIssuer) signAccessToken(
	userID int64,
	sessionID string,
	roleIDs []int64,
	isSuperAdmin bool,
	now time.Time,
) (string, error) {
	tokenID, err := randomString(i.random, tokenIDBytes)
	if err != nil {
		return "", fmt.Errorf("generate access token id: %w", err)
	}

	claims := accessClaims{
		UserID:       userID,
		SessionID:    sessionID,
		RoleIDs:      roleIDs,
		IsSuperAdmin: isSuperAdmin,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(i.config.AccessExpire)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    i.config.Issuer,
			Subject:   strconv.FormatInt(userID, 10),
			ID:        tokenID,
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).
		SignedString([]byte(i.config.AccessSecret))
	if err != nil {
		return "", fmt.Errorf("sign access token: %w", err)
	}
	return token, nil
}

func (i *TokenIssuer) credentials(accessToken, sessionID, refreshSecret string) *Credentials {
	// The session ID selects the Redis record and the random secret proves
	// possession; only the secret's SHA-256 hash is persisted in the session.
	return &Credentials{
		AccessToken:  accessToken,
		RefreshToken: sessionID + "." + refreshSecret,
		ExpiresIn:    int64(i.config.AccessExpire / time.Second),
	}
}

// accessClaims stores role membership and super-administrator status as
// issuance-time snapshots. Issue and Refresh both reload enabled roles from
// PostgreSQL before signing a new access token.
type accessClaims struct {
	UserID       int64   `json:"userId"`
	SessionID    string  `json:"sessionId"`
	RoleIDs      []int64 `json:"roleIds"`
	IsSuperAdmin bool    `json:"isSuperAdmin"`
	jwt.RegisteredClaims
}

func (i *TokenIssuer) roleIDs(ctx context.Context, userID int64) ([]int64, error) {
	roleIDs, err := i.roles.ListEnabledRoleIDs(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("load user roles: %w", err)
	}
	unique := make(map[int64]struct{}, len(roleIDs))
	normalized := make([]int64, 0, len(roleIDs))
	for _, roleID := range roleIDs {
		if roleID <= 0 {
			return nil, errors.New("role provider returned invalid role id")
		}
		if _, exists := unique[roleID]; exists {
			continue
		}
		unique[roleID] = struct{}{}
		normalized = append(normalized, roleID)
	}
	return normalized, nil
}

func (i *TokenIssuer) isSuperAdmin(ctx context.Context, userID int64) (bool, error) {
	isSuperAdmin, err := i.roles.IsSuperAdmin(ctx, userID)
	if err != nil {
		return false, fmt.Errorf("check super administrator: %w", err)
	}
	return isSuperAdmin, nil
}

func parseRefreshToken(value string) (string, string, error) {
	if len(value) > 256 {
		return "", "", ErrInvalidRefreshToken
	}
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return "", "", ErrInvalidRefreshToken
	}
	if !validRandomString(parts[0], sessionIDBytes) || !validRandomString(parts[1], refreshSecretBytes) {
		return "", "", ErrInvalidRefreshToken
	}
	return parts[0], parts[1], nil
}

func validRandomString(value string, size int) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == size
}

func hashRefreshSecret(secret string) string {
	hash := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(hash[:])
}

func randomString(reader io.Reader, size int) (string, error) {
	value := make([]byte, size)
	if _, err := io.ReadFull(reader, value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
