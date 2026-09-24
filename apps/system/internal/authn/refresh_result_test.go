package authn

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRefreshResultEncryptionAndBinding(t *testing.T) {
	issuer, _ := NewTokenIssuer(validTokenConfig(), &sessionStoreStub{}, testRoleProvider())
	now := time.Now().UTC().Truncate(time.Second)
	issuer.now = func() time.Time { return now }
	credentials := &Credentials{AccessToken: "access-secret", RefreshToken: "refresh-secret", ExpiresIn: 900}
	sealed, err := issuer.encryptRefreshResult("session-a", credentials, now.Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sealed, credentials.AccessToken) || strings.Contains(sealed, credentials.RefreshToken) {
		t.Fatal("plaintext persisted")
	}
	result, err := issuer.decryptRefreshResult("session-a", sealed)
	if err != nil || *result != *credentials {
		t.Fatalf("decrypt result: %v", err)
	}
	issuer.now = func() time.Time { return now.Add(3 * time.Second) }
	result, err = issuer.decryptRefreshResult("session-a", sealed)
	if err != nil || result.ExpiresIn != 897 {
		t.Fatalf("replay extended JWT expiry: %v", err)
	}
	if _, err := issuer.decryptRefreshResult("session-b", sealed); err == nil {
		t.Fatal("cross-session ciphertext accepted")
	}
	bytes, _ := base64.RawURLEncoding.DecodeString(sealed)
	bytes[len(bytes)-1] ^= 1
	for _, invalid := range []string{"", "!bad", base64.RawURLEncoding.EncodeToString(bytes)} {
		if _, err := issuer.decryptRefreshResult("session-a", invalid); err == nil {
			t.Fatal("invalid ciphertext accepted")
		}
	}
	issuer.now = func() time.Time { return now.Add(15 * time.Minute) }
	if _, err := issuer.decryptRefreshResult("session-a", sealed); err == nil {
		t.Fatal("expired access result accepted")
	}
	issuer.config.AccessSecret = strings.Repeat("x", 32)
	if _, err := issuer.decryptRefreshResult("session-a", sealed); err == nil {
		t.Fatal("different deployment key accepted")
	}
}

func TestRefreshReturnsWinningCredentialsWithoutExtendingSession(t *testing.T) {
	store := &sessionStoreStub{}
	issuer, _ := NewTokenIssuer(validTokenConfig(), store, testRoleProvider())
	now := time.Now().UTC().Truncate(time.Second)
	issuer.now = func() time.Time { return now }
	original, err := issuer.Issue(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	first, err := issuer.Refresh(context.Background(), original.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	firstSession := store.session
	now = now.Add(time.Second)
	for _, token := range []string{original.RefreshToken, first.RefreshToken, original.RefreshToken} {
		got, err := issuer.Refresh(context.Background(), token)
		if err != nil {
			t.Fatal(err)
		}
		if got.AccessToken != first.AccessToken || got.RefreshToken != first.RefreshToken || got.ExpiresIn != 899 {
			t.Fatal("retry did not use winning credentials")
		}
		if store.session != firstSession {
			t.Fatal("retry extended or changed session")
		}
	}
	if err := store.Revoke(context.Background(), 42, store.session.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := issuer.Refresh(context.Background(), original.RefreshToken); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("revoked session refreshed: %v", err)
	}
}

func TestRedisRotationWindowBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hash    string
		advance time.Duration
		reject  bool
	}{
		{"previous within window", "refresh-hash", 4999 * time.Millisecond, false},
		{"current within window", "next-hash", 4999 * time.Millisecond, false},
		{"previous at deadline", "refresh-hash", 5 * time.Second, true},
		{"unknown during window", "older-hash", time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newRedisSessionClientStub()
			client.now = time.Now()
			store, _ := NewRedisSessionStore(client, "session", "users")
			session := validSession("sid", 42)
			if err := store.Create(context.Background(), session, time.Hour); err != nil {
				t.Fatal(err)
			}
			first, err := store.RotateRefreshToken(context.Background(), "sid", "refresh-hash", "next-hash", session.ExpiresAt, time.Hour, "ciphertext-1")
			if err != nil {
				t.Fatal(err)
			}
			client.now = client.now.Add(tc.advance)
			got, err := store.RotateRefreshToken(context.Background(), "sid", tc.hash, "new-candidate", session.ExpiresAt.Add(time.Hour), 2*time.Hour, "ciphertext-2")
			if tc.reject {
				if !errors.Is(err, ErrRefreshTokenMismatch) {
					t.Fatalf("expected reuse rejection: %v", err)
				}
				if _, err := store.Get(context.Background(), "sid"); !errors.Is(err, ErrSessionNotFound) {
					t.Fatal("reuse did not revoke")
				}
			} else {
				if err != nil || *got != *first {
					t.Fatalf("retry changed result: %v", err)
				}
				if client.ttls["session:sid"] != 3600 {
					t.Fatal("retry extended session TTL")
				}
			}
		})
	}
}

func TestRedisRotationCurrentAfterWindowAndLegacySession(t *testing.T) {
	client := newRedisSessionClientStub()
	store, _ := NewRedisSessionStore(client, "session", "users")
	session := validSession("sid", 42)
	if err := store.Create(context.Background(), session, time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"refresh-hash", "one"}, {"one", "two"}} {
		if _, err := store.RotateRefreshToken(context.Background(), "sid", pair[0], pair[1], session.ExpiresAt, time.Hour, "cipher"); err != nil {
			t.Fatal(err)
		}
		client.now = client.now.Add(5 * time.Second)
	}
	var persisted Session
	if err := json.Unmarshal([]byte(client.values["session:sid"]), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.RefreshTokenHash != "two" || persisted.PreviousRefreshTokenHash != "one" {
		t.Fatal("rotation did not advance exactly one generation")
	}
}
