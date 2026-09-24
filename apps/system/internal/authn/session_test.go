package authn

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

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
		"encrypted-result",
	)
	if err != nil {
		t.Fatalf("rotate refresh token: %v", err)
	}
	if rotated.RefreshTokenHash != "next-hash" || !rotated.ExpiresAt.Equal(nextExpiry) {
		t.Fatalf("unexpected rotated session: %+v", rotated)
	}

	client.now = client.now.Add(5 * time.Second)
	_, err = store.RotateRefreshToken(
		context.Background(),
		session.ID,
		session.RefreshTokenHash,
		"another-hash",
		nextExpiry.Add(time.Hour),
		3*time.Hour,
		"other-encrypted-result",
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
				"encrypted-result",
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
			"encrypted-result",
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
		client.evalErr = redisErr
		_, err := store.RotateRefreshToken(
			context.Background(),
			session.ID,
			"reused-old-hash",
			"next-hash",
			session.ExpiresAt.Add(time.Hour),
			2*time.Hour,
			"encrypted-result",
		)
		if !errors.Is(err, redisErr) {
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
