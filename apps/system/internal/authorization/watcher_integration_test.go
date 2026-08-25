//go:build integration

package authorization

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

func TestRedisPolicyWatcherReceivesOrdinaryInvalidation(t *testing.T) {
	host := strings.TrimSpace(os.Getenv("DOGX_TEST_REDIS_HOST"))
	if host == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required for policy watcher integration test")
	}
	conf := redis.RedisConf{
		Host:               host,
		Type:               redis.NodeType,
		DisableIdentity:    true,
		MaintNotifications: "disabled",
	}
	channel := fmt.Sprintf("dogx:test:authorization:policy:%d", time.Now().UnixNano())
	received := make(chan struct{}, 1)
	subscriber, err := NewRedisPolicyWatcher(conf, channel, func(string) {
		select {
		case received <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatalf("create policy subscriber: %v", err)
	}
	t.Cleanup(subscriber.Close)
	publisher, err := NewRedisPolicyPublisher(conf, channel)
	if err != nil {
		t.Fatalf("create policy publisher: %v", err)
	}
	t.Cleanup(publisher.Close)

	if err := publisher.Update(); err != nil {
		t.Fatalf("publish policy invalidation: %v", err)
	}
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("policy subscriber did not receive invalidation")
	}
}
