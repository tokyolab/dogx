//go:build integration

package dictcache

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

func TestRedisDictionaryCacheLifecycle(t *testing.T) {
	host := os.Getenv("DOGX_TEST_REDIS_HOST")
	if host == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required")
	}
	client, err := redis.NewRedis(redis.RedisConf{Host: host, Pass: os.Getenv("DOGX_TEST_REDIS_PASS"), Type: redis.NodeType, DisableIdentity: true, MaintNotifications: "disabled"})
	if err != nil {
		t.Fatal(err)
	}
	c := New(client)
	c.prefix = fmt.Sprintf("dogx:test:{dict-%d}:", time.Now().UnixNano())
	ctx := context.Background()
	unrelated := c.prefix + "outside"
	otherKey := "dogx:test:other:" + fmt.Sprint(time.Now().UnixNano())
	t.Cleanup(func() {
		_ = c.Clear(context.Background())
		_, _ = client.DelCtx(context.Background(), unrelated, otherKey, c.index())
	})
	if err := client.SetexCtx(ctx, otherKey, "keep", 60); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "empty", Entry{Status: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "orphan", Entry{Status: 0, IsPublic: true, Items: []Item{{Label: "旧标签", Value: "old", Status: 0}}}); err != nil {
		t.Fatal(err)
	}
	ttl, err := client.TtlCtx(ctx, c.key("empty"))
	if err != nil || ttl < 86395 || ttl > 86400 {
		t.Fatal(ttl, err)
	}
	got, err := c.Read(ctx, []string{"empty", "orphan", "missing"})
	if err != nil || len(got) != 2 || got["empty"].Items == nil || got["orphan"].Items[0].Label != "旧标签" {
		t.Fatal(got, err)
	}
	if err := c.Invalidate(ctx, "empty"); err != nil {
		t.Fatal(err)
	}
	// Clearing uses only the index, so an entry with no database row is still removed.
	if err := client.SetexCtx(ctx, unrelated, "keep-unindexed", 60); err != nil {
		t.Fatal(err)
	}
	if err := c.Clear(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Read(ctx, []string{"empty", "orphan"}); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	if value, err := client.GetCtx(ctx, unrelated); err != nil || value != "keep-unindexed" {
		t.Fatal(value, err)
	}
	if value, err := client.GetCtx(ctx, otherKey); err != nil || value != "keep" {
		t.Fatal(value, err)
	}
	if count, err := client.ScardCtx(ctx, c.index()); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}
