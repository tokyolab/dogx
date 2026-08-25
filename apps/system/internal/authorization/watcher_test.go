package authorization

import (
	"testing"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

func TestWatcherOptionsUseRedisCredentialsAndChannel(t *testing.T) {
	options, err := watcherOptions(redis.RedisConf{
		Host: "127.0.0.1:6379",
		Type: redis.NodeType,
		User: "dogx",
		Pass: "secret",
	}, "dogx:test:policy")
	if err != nil {
		t.Fatalf("build watcher options: %v", err)
	}
	if options.Channel != "dogx:test:policy" || options.Options.Username != "dogx" || options.Options.Password != "secret" {
		t.Fatalf("unexpected watcher options: %+v", options)
	}
	if _, err := watcherOptions(redis.RedisConf{}, "channel"); err == nil {
		t.Fatal("expected invalid Redis config to be rejected")
	}
	if _, err := watcherOptions(redis.RedisConf{Host: "127.0.0.1:6379", Type: redis.NodeType}, " "); err == nil {
		t.Fatal("expected empty channel to be rejected")
	}
}
