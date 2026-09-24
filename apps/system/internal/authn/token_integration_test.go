//go:build integration

package authn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

func TestRedisSessionRefreshRotationAndReuseRevocation(t *testing.T) {
	redisHost := strings.TrimSpace(os.Getenv("DOGX_TEST_REDIS_HOST"))
	if redisHost == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required for authentication integration test")
	}
	client, err := redis.NewRedis(redis.RedisConf{
		Host:               redisHost,
		Pass:               os.Getenv("DOGX_TEST_REDIS_PASS"),
		Type:               redis.NodeType,
		PingTimeout:        time.Second,
		DisableIdentity:    true,
		MaintNotifications: "disabled",
	})
	if err != nil {
		t.Fatalf("create Redis client: %v", err)
	}

	prefix := fmt.Sprintf("dogx:test:auth:%d", time.Now().UnixNano())
	store, err := NewRedisSessionStore(client, prefix+":session", prefix+":user_sessions")
	if err != nil {
		t.Fatalf("create session store: %v", err)
	}
	issuer, err := NewTokenIssuer(TokenConfig{
		AccessSecret:  "0123456789abcdef0123456789abcdef",
		AccessExpire:  time.Minute,
		RefreshExpire: time.Hour,
		Issuer:        "dogx-integration-test",
	}, store, &roleProviderStub{roleIDs: []int64{1}})
	if err != nil {
		t.Fatalf("create token issuer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	original, err := issuer.Issue(ctx, 42)
	if err != nil {
		t.Fatalf("issue credentials: %v", err)
	}
	sessionID := strings.Split(original.RefreshToken, ".")[0]
	cleanupSessionIDs := []string{sessionID}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		keys := []string{prefix + ":user_sessions:42"}
		for _, id := range cleanupSessionIDs {
			keys = append(keys, prefix+":session:"+id)
		}
		_, cleanupErr := client.DelCtx(cleanupCtx, keys...)
		if cleanupErr != nil {
			t.Errorf("delete authentication test keys: %v", cleanupErr)
		}
	})

	var wait sync.WaitGroup
	results := make(chan *Credentials, 12)
	start := make(chan struct{})
	for index := 0; index < 12; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			retry, err := issuer.Refresh(ctx, original.RefreshToken)
			if err != nil {
				t.Errorf("concurrent refresh: %v", err)
				return
			}
			results <- retry
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	var refreshed *Credentials
	for result := range results {
		if refreshed == nil {
			refreshed = result
		}
		if result.AccessToken != refreshed.AccessToken || result.RefreshToken != refreshed.RefreshToken {
			t.Error("concurrent refresh returned another token")
		}
	}
	if t.Failed() || refreshed == nil {
		t.FailNow()
	}
	if refreshed.RefreshToken == original.RefreshToken {
		t.Fatal("refresh token was not rotated")
	}
	beforeRetry, err := store.Get(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	beforeTTL, err := client.TtlCtx(ctx, prefix+":session:"+sessionID)
	if err != nil {
		t.Fatal(err)
	}
	current, err := issuer.Refresh(ctx, refreshed.RefreshToken)
	if err != nil || current.RefreshToken != refreshed.RefreshToken {
		t.Fatalf("current token retry: %v", err)
	}
	// Advance only this test's replay deadline, avoiding sleeps or changes to the
	// server clock. Production always obtains its deadline from Redis TIME.
	session, err := store.Get(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if *beforeRetry != *session {
		t.Fatal("retries changed session or replay deadline")
	}
	afterTTL, err := client.TtlCtx(ctx, prefix+":session:"+sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if afterTTL > beforeTTL {
		t.Fatal("retry extended session TTL")
	}
	other, err := issuer.Issue(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	otherID, _, _ := strings.Cut(other.RefreshToken, ".")
	cleanupSessionIDs = append(cleanupSessionIDs, otherID)
	session.RefreshReplayUntil = 1
	encoded, err := json.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetexCtx(ctx, prefix+":session:"+sessionID, string(encoded), 3600); err != nil {
		t.Fatal(err)
	}
	if _, err := issuer.Refresh(ctx, original.RefreshToken); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("expected old refresh token reuse to be rejected, got: %v", err)
	}
	if _, err := store.Get(ctx, sessionID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("refresh token reuse did not revoke session: %v", err)
	}
	if _, err := issuer.Refresh(ctx, refreshed.RefreshToken); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("replay resurrected revoked session: %v", err)
	}
	otherRefreshed, err := issuer.Refresh(ctx, other.RefreshToken)
	if err != nil {
		t.Fatalf("reuse affected independent login: %v", err)
	}
	if err := store.RevokeAll(ctx, 42); err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{other.RefreshToken, otherRefreshed.RefreshToken} {
		if _, err := issuer.Refresh(ctx, token); !errors.Is(err, ErrInvalidRefreshToken) {
			t.Fatalf("bulk logout replayed cached result: %v", err)
		}
	}
}
