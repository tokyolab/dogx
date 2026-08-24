package authn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

const testAccessSecret = "0123456789abcdef0123456789abcdef"

type sessionStoreStub struct {
	session Session
	ttl     time.Duration
	err     error
	revoked bool
}

func (s *sessionStoreStub) Create(_ context.Context, session Session, ttl time.Duration) error {
	s.session = session
	s.ttl = ttl
	return s.err
}

func (s *sessionStoreStub) Get(_ context.Context, sessionID string) (*Session, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.revoked || s.session.ID == "" || s.session.ID != sessionID {
		return nil, ErrSessionNotFound
	}
	copy := s.session
	return &copy, nil
}

func (s *sessionStoreStub) RotateRefreshToken(
	_ context.Context,
	sessionID string,
	currentHash string,
	nextHash string,
	expiresAt time.Time,
	ttl time.Duration,
) (*Session, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.revoked || s.session.ID != sessionID {
		return nil, ErrSessionNotFound
	}
	if s.session.RefreshTokenHash != currentHash {
		s.revoked = true
		return nil, ErrRefreshTokenMismatch
	}
	s.session.RefreshTokenHash = nextHash
	s.session.ExpiresAt = expiresAt
	s.ttl = ttl
	copy := s.session
	return &copy, nil
}

func (s *sessionStoreStub) Revoke(_ context.Context, userID int64, sessionID string) error {
	if s.session.UserID != userID || s.session.ID != sessionID {
		return ErrSessionUserMismatch
	}
	s.revoked = true
	return nil
}

func (s *sessionStoreStub) RevokeAll(_ context.Context, userID int64) error {
	if s.session.UserID == userID {
		s.revoked = true
	}
	return nil
}

func TestTokenIssuerIssue(t *testing.T) {
	store := &sessionStoreStub{}
	issuer, err := NewTokenIssuer(validTokenConfig(), store)
	if err != nil {
		t.Fatalf("create token issuer: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	issuer.now = func() time.Time { return now }
	issuer.random = bytes.NewReader(make([]byte, sessionIDBytes+refreshSecretBytes+tokenIDBytes))

	credentials, err := issuer.Issue(context.Background(), 42)
	if err != nil {
		t.Fatalf("issue credentials: %v", err)
	}
	if credentials.ExpiresIn != 15*60 {
		t.Fatalf("unexpected access expiry: %d", credentials.ExpiresIn)
	}

	parts := strings.Split(credentials.RefreshToken, ".")
	if len(parts) != 2 || parts[0] != store.session.ID {
		t.Fatalf("unexpected refresh token format: %s", credentials.RefreshToken)
	}
	expectedRefreshHash := sha256.Sum256([]byte(parts[1]))
	if store.session.RefreshTokenHash != hex.EncodeToString(expectedRefreshHash[:]) {
		t.Fatal("stored refresh token hash does not match returned secret")
	}
	if store.session.UserID != 42 || store.ttl != 7*24*time.Hour {
		t.Fatalf("unexpected session: %+v ttl=%s", store.session, store.ttl)
	}
	if !store.session.CreatedAt.Equal(now) || !store.session.ExpiresAt.Equal(now.Add(store.ttl)) {
		t.Fatalf("unexpected session timestamps: %+v", store.session)
	}

	parsed, err := jwt.ParseWithClaims(
		credentials.AccessToken,
		&accessClaims{},
		func(token *jwt.Token) (any, error) {
			if token.Method != jwt.SigningMethodHS256 {
				t.Fatalf("unexpected signing method: %s", token.Method.Alg())
			}
			return []byte(testAccessSecret), nil
		},
	)
	if err != nil || !parsed.Valid {
		t.Fatalf("parse access token: %v", err)
	}
	claims := parsed.Claims.(*accessClaims)
	if claims.UserID != 42 || claims.SessionID != store.session.ID || claims.Issuer != "dogx-test" {
		t.Fatalf("unexpected access claims: %+v", claims)
	}
	if claims.ExpiresAt == nil || !claims.ExpiresAt.Time.Equal(now.Add(15*time.Minute)) {
		t.Fatalf("unexpected access expiry claim: %+v", claims.ExpiresAt)
	}
}

func TestTokenIssuerRefreshRotatesTokenAndRejectsReuse(t *testing.T) {
	store := &sessionStoreStub{}
	issuer, err := NewTokenIssuer(validTokenConfig(), store)
	if err != nil {
		t.Fatalf("create token issuer: %v", err)
	}
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	issuer.now = func() time.Time { return now }
	issuer.random = bytes.NewReader(make([]byte, sessionIDBytes+refreshSecretBytes+tokenIDBytes))
	original, err := issuer.Issue(context.Background(), 42)
	if err != nil {
		t.Fatalf("issue credentials: %v", err)
	}

	issuer.now = func() time.Time { return now.Add(time.Minute) }
	issuer.random = bytes.NewReader(bytes.Repeat([]byte{1}, refreshSecretBytes+tokenIDBytes))
	refreshed, err := issuer.Refresh(context.Background(), original.RefreshToken)
	if err != nil {
		t.Fatalf("refresh credentials: %v", err)
	}
	if refreshed.RefreshToken == original.RefreshToken || refreshed.AccessToken == original.AccessToken {
		t.Fatal("refresh did not rotate both credentials")
	}
	if !store.session.ExpiresAt.Equal(now.Add(time.Minute).Add(7 * 24 * time.Hour)) {
		t.Fatalf("refresh expiry was not extended: %s", store.session.ExpiresAt)
	}

	issuer.random = bytes.NewReader(bytes.Repeat([]byte{2}, refreshSecretBytes+tokenIDBytes))
	if _, err := issuer.Refresh(context.Background(), original.RefreshToken); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("expected old refresh token reuse to be rejected, got: %v", err)
	}
	if !store.revoked {
		t.Fatal("refresh token reuse did not revoke the session")
	}
}

func TestTokenIssuerRejectsMalformedAndUnknownRefreshTokens(t *testing.T) {
	store := &sessionStoreStub{}
	issuer, err := NewTokenIssuer(validTokenConfig(), store)
	if err != nil {
		t.Fatalf("create token issuer: %v", err)
	}
	for _, token := range []string{"", "invalid", "a.b", strings.Repeat("a", 257)} {
		if _, err := issuer.Refresh(context.Background(), token); !errors.Is(err, ErrInvalidRefreshToken) {
			t.Fatalf("expected malformed token to be rejected, token=%q err=%v", token, err)
		}
	}

	unknownSession, _ := randomString(bytes.NewReader(make([]byte, sessionIDBytes)), sessionIDBytes)
	secret, _ := randomString(bytes.NewReader(make([]byte, refreshSecretBytes)), refreshSecretBytes)
	if _, err := issuer.Refresh(context.Background(), unknownSession+"."+secret); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("expected unknown session to be rejected, got: %v", err)
	}
}

func TestTokenIssuerValidatesConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TokenConfig)
		store  SessionStore
	}{
		{name: "short secret", mutate: func(c *TokenConfig) { c.AccessSecret = "short" }, store: &sessionStoreStub{}},
		{name: "invalid access expiry", mutate: func(c *TokenConfig) { c.AccessExpire = 0 }, store: &sessionStoreStub{}},
		{name: "invalid refresh expiry", mutate: func(c *TokenConfig) { c.RefreshExpire = c.AccessExpire }, store: &sessionStoreStub{}},
		{name: "empty issuer", mutate: func(c *TokenConfig) { c.Issuer = "" }, store: &sessionStoreStub{}},
		{name: "nil session store", mutate: func(*TokenConfig) {}, store: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validTokenConfig()
			test.mutate(&config)
			if _, err := NewTokenIssuer(config, test.store); err == nil {
				t.Fatal("expected invalid configuration to be rejected")
			}
		})
	}
}

func TestTokenIssuerReturnsGenerationAndStorageErrors(t *testing.T) {
	issuer, err := NewTokenIssuer(validTokenConfig(), &sessionStoreStub{})
	if err != nil {
		t.Fatalf("create token issuer: %v", err)
	}
	issuer.random = strings.NewReader("")
	if _, err := issuer.Issue(context.Background(), 1); err == nil {
		t.Fatal("expected random source error")
	}
	if _, err := issuer.Issue(context.Background(), 0); err == nil {
		t.Fatal("expected invalid user id error")
	}

	storeErr := errors.New("session store unavailable")
	issuer, err = NewTokenIssuer(validTokenConfig(), &sessionStoreStub{err: storeErr})
	if err != nil {
		t.Fatalf("create token issuer: %v", err)
	}
	if _, err := issuer.Issue(context.Background(), 1); !errors.Is(err, storeErr) {
		t.Fatalf("expected session store error, got: %v", err)
	}
}

func validTokenConfig() TokenConfig {
	return TokenConfig{
		AccessSecret:  testAccessSecret,
		AccessExpire:  15 * time.Minute,
		RefreshExpire: 7 * 24 * time.Hour,
		Issuer:        "dogx-test",
	}
}
