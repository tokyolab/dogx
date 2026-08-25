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

type PolicyNotifier interface {
	Update() error
}

type PolicyWatcher interface {
	persist.Watcher
	Close()
}

type managedPolicyWatcher struct {
	persist.Watcher
	close func()
	once  sync.Once
}

func (w *managedPolicyWatcher) Close() {
	w.once.Do(w.close)
}

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
