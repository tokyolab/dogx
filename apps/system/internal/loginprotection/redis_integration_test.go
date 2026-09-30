//go:build integration

package loginprotection

import (
	"context"
	"errors"
	"fmt"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/zeromicro/go-zero/core/limit"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testRedis(t *testing.T) (*redis.Redis, redis.RedisConf) {
	t.Helper()
	host := os.Getenv("DOGX_TEST_REDIS_HOST")
	if host == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required")
	}
	conf := redis.RedisConf{Host: host, Pass: os.Getenv("DOGX_TEST_REDIS_PASS"), Type: redis.NodeType, DisableIdentity: true, MaintNotifications: "disabled"}
	client, err := redis.NewRedis(conf)
	if err != nil {
		t.Fatal(err)
	}
	return client, conf
}
func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition timed out")
		case <-tick.C:
		}
	}
}

func TestRedisFailureWindowsAndConcurrency(t *testing.T) {
	client, _ := testRedis(t)
	store := NewFailureStore(client)
	ctx := context.Background()
	username := fmt.Sprintf("security-test-%d", time.Now().UnixNano())
	key := failureKey(username)
	t.Cleanup(func() { _, _ = client.Del(key) })
	cfg := validConfig()
	cfg.FailureThreshold = 20
	if locked, err := store.Locked(ctx, username); err != nil || locked {
		t.Fatalf("%v %v", locked, err)
	}
	if err := store.Fail(ctx, username, cfg); err != nil {
		t.Fatal(err)
	}
	// Shorten only this test's exact key to verify that retries preserve its TTL.
	if _, err := client.EvalCtx(ctx, "return redis.call('PEXPIRE', KEYS[1], 1500)", []string{key}); err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(ctx, strings.ToUpper(username), cfg); err != nil {
		t.Fatal(err)
	}
	ttl, err := client.EvalCtx(ctx, "return redis.call('PTTL', KEYS[1])", []string{key})
	if err != nil || ttl.(int64) > 1500 {
		t.Fatalf("extended window %v %v", ttl, err)
	}
	if err := store.Clear(ctx, username); err != nil {
		t.Fatal(err)
	}
	if exists, _ := client.Exists(key); exists {
		t.Fatal("successful login did not clear counter")
	}
	// Two instances share the same account state; concurrent failures cannot be lost.
	second := NewFailureStore(client)
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := second.Fail(ctx, username, cfg); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	if locked, err := store.Locked(ctx, username); err != nil || !locked {
		t.Fatalf("threshold lost: %v %v", locked, err)
	}
	if err := store.Clear(ctx, username); err != nil {
		t.Fatal(err)
	}
	if locked, _ := store.Locked(ctx, username); !locked {
		t.Fatal("in-flight success erased concurrent lock")
	}
	if _, err := client.EvalCtx(ctx, "return redis.call('PEXPIRE', KEYS[1], 120)", []string{key}); err != nil {
		t.Fatal(err)
	}
	cfg.LockDurationSeconds = 3600
	if err := store.Fail(ctx, username, cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { locked, err := store.Locked(ctx, username); return err == nil && !locked })
	cfg.FailureThreshold = 2
	if err := store.Fail(ctx, username, cfg); err != nil {
		t.Fatal(err)
	}
	if locked, _ := store.Locked(ctx, username); locked {
		t.Fatal("expired count reused")
	}
}

func TestRedisPeriodLimitBoundaries(t *testing.T) {
	client, _ := testRedis(t)
	prefix := fmt.Sprintf("dogx:test:rate:%d:", time.Now().UnixNano())
	ctx := context.Background()
	t.Cleanup(func() { _, _ = client.Del(prefix+"login:ip-a", prefix+"login:ip-b", prefix+"captcha:ip-a") })
	limiter := limit.NewPeriodLimit(60, 2, client, prefix)
	for _, want := range []int{limit.Allowed, limit.HitQuota, limit.OverQuota} {
		got, err := limiter.TakeCtx(ctx, "login:ip-a")
		if err != nil || got != want {
			t.Fatalf("%v %v want %v", got, err, want)
		}
	}
	for _, key := range []string{"login:ip-b", "captcha:ip-a"} {
		if got, err := limiter.TakeCtx(ctx, key); err != nil || got != limit.Allowed {
			t.Fatalf("quotas coupled %v %v", got, err)
		}
	}
	if _, err := client.EvalCtx(ctx, "return redis.call('PEXPIRE', KEYS[1], 120)", []string{prefix + "login:ip-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.TakeCtx(ctx, "login:ip-a"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool { exists, err := client.Exists(prefix + "login:ip-a"); return err == nil && !exists })
	if got, err := limiter.TakeCtx(ctx, "login:ip-a"); err != nil || got != limit.Allowed {
		t.Fatalf("window not reset %v %v", got, err)
	}
}

func TestRuntimeNotificationAndMissedNotificationFallback(t *testing.T) {
	_, conf := testRedis(t)
	ctx := context.Background()
	var maximum atomic.Int32
	maximum.Store(30)
	load := func(context.Context) (*system.LoginSecurityConfig, error) {
		cfg := validConfig()
		cfg.RateLimitMaxRequests = maximum.Load()
		return cfg, nil
	}
	first, err := NewRuntime(conf, load)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewLazyRuntime(conf, load)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if second.value.Load() != nil {
		t.Fatal("lazy startup populated the cache")
	}
	if cfg, err := second.Current(ctx); err != nil || cfg.RateLimitMaxRequests != 30 {
		t.Fatalf("first demand load: %v %v", cfg, err)
	}
	maximum.Store(41)
	if err := first.Notify(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		a, e1 := first.Current(ctx)
		b, e2 := second.Current(ctx)
		return e1 == nil && e2 == nil && a.RateLimitMaxRequests == 41 && b.RateLimitMaxRequests == 41
	})
	maximum.Store(42) // Deliberately no notification: exercise the actual periodic fallback.
	waitFor(t, ReloadInterval+5*time.Second, func() bool {
		a, e1 := first.Current(ctx)
		b, e2 := second.Current(ctx)
		return e1 == nil && e2 == nil && a.RateLimitMaxRequests == 42 && b.RateLimitMaxRequests == 42
	})
	if _, err := NewRuntime(conf, func(context.Context) (*system.LoginSecurityConfig, error) { return nil, errors.New("db unavailable") }); err == nil {
		t.Fatal("startup accepted missing config")
	}
}

func TestLazyRuntimeStartsWithoutConfigurationAndRecovers(t *testing.T) {
	_, conf := testRedis(t)
	var calls atomic.Int32
	var available atomic.Bool
	r, err := NewLazyRuntime(conf, func(context.Context) (*system.LoginSecurityConfig, error) {
		calls.Add(1)
		if !available.Load() {
			return nil, errors.New("RPC offline")
		}
		return validConfig(), nil
	})
	if err != nil {
		t.Fatalf("lazy startup depended on RPC: %v", err)
	}
	defer r.Close()
	if calls.Load() != 0 {
		t.Fatal("startup attempted configuration load")
	}
	if _, err := r.Current(context.Background()); err == nil {
		t.Fatal("login accepted missing configuration")
	}
	available.Store(true)
	waitFor(t, 3*time.Second, func() bool {
		cfg, err := r.Current(context.Background())
		return err == nil && cfg.FailureThreshold == 5
	})
	if calls.Load() != 2 {
		t.Fatalf("unexpected RPC calls during backoff: %d", calls.Load())
	}
}
