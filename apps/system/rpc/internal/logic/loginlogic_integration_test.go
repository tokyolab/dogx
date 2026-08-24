//go:build integration

package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

func TestLoginUsesPostgreSQLPasswordHashAndRedisSession(t *testing.T) {
	gormDB, sqlDB := testutil.OpenPostgres(t)
	provider, err := migration.NewProvider(sqlDB)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	redisHost := strings.TrimSpace(os.Getenv("DOGX_TEST_REDIS_HOST"))
	if redisHost == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required for login integration test")
	}
	redisClient, err := redis.NewRedis(redis.RedisConf{
		Host:               redisHost,
		Type:               redis.NodeType,
		PingTimeout:        time.Second,
		DisableIdentity:    true,
		MaintNotifications: "disabled",
	})
	if err != nil {
		t.Fatalf("create Redis client: %v", err)
	}
	prefix := fmt.Sprintf("dogx:test:login:%d", time.Now().UnixNano())

	hasher := authn.NewArgon2id()
	passwordHash, err := hasher.Hash("secure-password")
	if err != nil {
		t.Fatalf("hash test password: %v", err)
	}
	user := &model.User{
		Username:     "IntegrationAdmin",
		PasswordHash: passwordHash,
		Nickname:     "Integration Admin",
		Status:       model.RecordStatusEnabled,
	}
	userRepo := repository.NewUserRepository(gormDB)
	if err := userRepo.Create(ctx, user); err != nil {
		t.Fatalf("create login user: %v", err)
	}

	sessions, err := authn.NewRedisSessionStore(redisClient, prefix, prefix+":users")
	if err != nil {
		t.Fatalf("create session store: %v", err)
	}
	tokens, err := authn.NewTokenIssuer(authn.TokenConfig{
		AccessSecret:  "0123456789abcdef0123456789abcdef",
		AccessExpire:  15 * time.Minute,
		RefreshExpire: 7 * 24 * time.Hour,
		Issuer:        "dogx-integration-test",
	}, sessions)
	if err != nil {
		t.Fatalf("create token issuer: %v", err)
	}
	login := NewLoginLogic(ctx, &svc.ServiceContext{
		UserRepo:  userRepo,
		Passwords: hasher,
		Tokens:    tokens,
	})

	response, err := login.Login(&system.LoginRequest{
		Username: "integrationadmin",
		Password: "secure-password",
	})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if response.AccessToken == "" || response.ExpiresIn != 900 {
		t.Fatalf("unexpected access credentials: %+v", response)
	}
	refreshParts := strings.Split(response.RefreshToken, ".")
	if len(refreshParts) != 2 {
		t.Fatalf("unexpected refresh token: %s", response.RefreshToken)
	}
	stored, err := redisClient.GetCtx(ctx, prefix+":"+refreshParts[0])
	if err != nil {
		t.Fatalf("read stored login session: %v", err)
	}
	var session authn.Session
	if err := json.Unmarshal([]byte(stored), &session); err != nil {
		t.Fatalf("decode stored login session: %v", err)
	}
	if session.UserID != user.ID || session.RefreshTokenHash == "" {
		t.Fatalf("unexpected stored login session: %+v", session)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, cleanupErr := redisClient.DelCtx(
			cleanupCtx,
			prefix+":"+session.ID,
			fmt.Sprintf("%s:users:%d", prefix, user.ID),
		)
		if cleanupErr != nil {
			t.Errorf("delete login test session: %v", cleanupErr)
		}
	})

	_, err = login.Login(&system.LoginRequest{
		Username: user.Username,
		Password: "wrong-password",
	})
	if _, ok := bizerror.From(err); !ok {
		t.Fatalf("wrong password did not return business error: %v", err)
	}
}
