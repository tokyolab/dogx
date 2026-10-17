package authorization

import (
	"crypto/tls"
	"errors"
	"strings"
	"sync"

	"github.com/casbin/casbin/v3/persist"
	rediswatcher "github.com/casbin/redis-watcher/v2"
	redisv9 "github.com/redis/go-redis/v9"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

// PolicyNotifier publishes hints that policy may have changed; persisted policy
// remains the authoritative source.
type PolicyNotifier interface {
	// Update must be called after policy changes commit. A nil error does not
	// mean other nodes have loaded the policy. Notifications may be missed and
	// do not replace periodic reconciliation or the database commit result.
	Update() error
}

// PolicyWatcher adds explicit shutdown to Casbin's notification interface.
// The caller owns its lifecycle.
type PolicyWatcher interface {
	persist.Watcher
	// Close initiates shutdown of the underlying watcher. Watchers constructed
	// by this package allow repeated calls, but returning does not guarantee
	// callbacks have exited or underlying clients have closed; it is not a join.
	Close()
}

type managedPolicyWatcher struct {
	persist.Watcher
	close func()
	once  sync.Once
}

func (w *managedPolicyWatcher) Close() {
	// redis-watcher's Close closes an internal channel and panics if repeated.
	// once prevents repeated calls; it does not turn asynchronous shutdown into
	// a synchronous completion guarantee.
	w.once.Do(w.close)
}

// NewRedisPolicyWatcher creates a watcher for external policy changes and ignores
// its own published messages. callback receives refresh hints, not policy data.
// On success, the caller must close the watcher.
func NewRedisPolicyWatcher(
	conf zeroredis.RedisConf,
	channel string,
	callback func(string),
) (PolicyWatcher, error) {
	if callback == nil {
		return nil, errors.New("policy watcher callback is nil")
	}
	options, err := watcherOptions(conf, channel)
	if err != nil {
		return nil, err
	}
	options.IgnoreSelf = true
	options.OptionalUpdateCallback = callback

	var watcher persist.Watcher
	if conf.Type == zeroredis.ClusterType {
		watcher, err = rediswatcher.NewWatcherWithCluster(conf.Host, options)
	} else {
		watcher, err = rediswatcher.NewWatcher(conf.Host, options)
	}
	if err != nil {
		return nil, err
	}
	return concreteWatcher(watcher)
}

// NewRedisPolicyPublisher creates a watcher used only to publish notifications;
// its receive callback does not process policy. On success, the caller must call
// Close; see PolicyWatcher for shutdown semantics.
func NewRedisPolicyPublisher(conf zeroredis.RedisConf, channel string) (PolicyWatcher, error) {
	options, err := watcherOptions(conf, channel)
	if err != nil {
		return nil, err
	}
	options.IgnoreSelf = true
	options.OptionalUpdateCallback = func(string) {}

	var watcher persist.Watcher
	if conf.Type == zeroredis.ClusterType {
		watcher, err = rediswatcher.NewWatcherWithCluster(conf.Host, options)
	} else {
		watcher, err = rediswatcher.NewPublishWatcher(conf.Host, options)
	}
	if err != nil {
		return nil, err
	}
	return concreteWatcher(watcher)
}

func watcherOptions(conf zeroredis.RedisConf, channel string) (rediswatcher.WatcherOptions, error) {
	if err := conf.Validate(); err != nil {
		return rediswatcher.WatcherOptions{}, err
	}
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return rediswatcher.WatcherOptions{}, errors.New("policy watcher channel is empty")
	}

	options := rediswatcher.WatcherOptions{
		Channel: channel,
		Options: redisv9.Options{
			Username: conf.User,
			Password: conf.Pass,
		},
		ClusterOptions: redisv9.ClusterOptions{
			Username: conf.User,
			Password: conf.Pass,
		},
	}
	if conf.Tls {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		options.Options.TLSConfig = tlsConfig
		options.ClusterOptions.TLSConfig = tlsConfig
	}
	return options, nil
}

func concreteWatcher(watcher persist.Watcher) (PolicyWatcher, error) {
	concrete, ok := watcher.(interface{ Close() })
	if !ok {
		return nil, errors.New("Casbin Redis watcher does not support close")
	}
	return &managedPolicyWatcher{Watcher: watcher, close: concrete.Close}, nil
}
