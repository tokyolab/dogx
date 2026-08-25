//go:build integration

package authn

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
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
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, cleanupErr := client.DelCtx(
			cleanupCtx,
			prefix+":session:"+sessionID,
			prefix+":user_sessions:42",
		)
		if cleanupErr != nil {
			t.Errorf("delete authentication test keys: %v", cleanupErr)
		}
	})

	refreshed, err := issuer.Refresh(ctx, original.RefreshToken)
	if err != nil {
		t.Fatalf("refresh credentials: %v", err)
	}
	if refreshed.RefreshToken == original.RefreshToken {
		t.Fatal("refresh token was not rotated")
	}
	if _, err := store.Get(ctx, sessionID); err != nil {
		t.Fatalf("rotated session was not stored: %v", err)
	}

	if _, err := issuer.Refresh(ctx, original.RefreshToken); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("expected old refresh token reuse to be rejected, got: %v", err)
	}
	if _, err := store.Get(ctx, sessionID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("refresh token reuse did not revoke session: %v", err)
	}
}
