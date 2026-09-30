package loginprotection

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	redisv9 "github.com/redis/go-redis/v9"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/zeromicro/go-zero/core/logx"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

const ConfigChannel = "dogx:security:config:changed"
const ReloadInterval = 30 * time.Second
const loadRetryDelay = time.Second

type Runtime struct {
	load         func(context.Context) (*system.LoginSecurityConfig, error)
	value        atomic.Pointer[system.LoginSecurityConfig]
	mu           sync.Mutex
	lazy         bool
	ctx          context.Context
	loads        singleflight.Group
	retryAt      time.Time
	loadErr      error
	client       redisv9.UniversalClient
	subscription *redisv9.PubSub
	cancel       context.CancelFunc
	done         chan struct{}
	closeOnce    sync.Once
}

// NewRuntime eagerly loads the RPC process's database-backed configuration.
func NewRuntime(conf zeroredis.RedisConf, load func(context.Context) (*system.LoginSecurityConfig, error)) (*Runtime, error) {
	return newRuntime(conf, load, false)
}

// NewLazyRuntime lets API startup proceed without a successful configuration RPC.
// Subscription still precedes all loads; notifications contain no configuration data.
func NewLazyRuntime(conf zeroredis.RedisConf, load func(context.Context) (*system.LoginSecurityConfig, error)) (*Runtime, error) {
	return newRuntime(conf, load, true)
}

func newRuntime(conf zeroredis.RedisConf, load func(context.Context) (*system.LoginSecurityConfig, error), lazy bool) (*Runtime, error) {
	if load == nil {
		return nil, errors.New("security configuration loader is nil")
	}
	if err := conf.Validate(); err != nil {
		return nil, err
	}
	var tlsConfig *tls.Config
	if conf.Tls {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	var client redisv9.UniversalClient
	if conf.Type == zeroredis.ClusterType {
		client = redisv9.NewClusterClient(&redisv9.ClusterOptions{Addrs: []string{conf.Host}, Username: conf.User, Password: conf.Pass, TLSConfig: tlsConfig})
	} else {
		client = redisv9.NewClient(&redisv9.Options{Addr: conf.Host, Username: conf.User, Password: conf.Pass, TLSConfig: tlsConfig})
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Runtime{load: load, client: client, ctx: ctx, lazy: lazy, cancel: cancel, done: make(chan struct{})}
	r.subscription = client.Subscribe(ctx, ConfigChannel)
	initial, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if _, err := r.subscription.Receive(initial); err != nil {
		cancel()
		_ = r.subscription.Close()
		_ = client.Close()
		return nil, err
	}
	if !lazy {
		if err := r.Reload(initial); err != nil {
			cancel()
			_ = r.subscription.Close()
			_ = client.Close()
			return nil, err
		}
	}
	go r.run(ctx)
	return r, nil
}

func (r *Runtime) Current(ctx context.Context) (*system.LoginSecurityConfig, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.lazy && r.ctx.Err() != nil {
		return nil, r.ctx.Err()
	}
	cfg := r.value.Load()
	if cfg == nil {
		if r.lazy {
			return r.loadOnDemand(ctx)
		}
		return nil, errors.New("login protection configuration unavailable")
	}
	return proto.Clone(cfg).(*system.LoginSecurityConfig), nil
}

func (r *Runtime) loadOnDemand(ctx context.Context) (*system.LoginSecurityConfig, error) {
	result := r.loads.DoChan("config", func() (any, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		// A notification or another load may have filled the cache while we waited.
		if cfg := r.value.Load(); cfg != nil {
			return cfg, nil
		}
		if time.Now().Before(r.retryAt) {
			return nil, r.loadErr
		}
		// One cancelled HTTP request must not cancel the shared load for other
		// callers. Runtime shutdown and a bounded timeout still cancel the RPC.
		attempt, cancel := context.WithTimeout(r.ctx, 5*time.Second)
		defer cancel()
		if err := r.reloadLocked(attempt); err != nil {
			logx.Errorf("load login security configuration on demand: %v", err)
			return nil, err
		}
		return r.value.Load(), nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case loaded := <-result:
		if loaded.Err != nil {
			return nil, loaded.Err
		}
		return proto.Clone(loaded.Val.(*system.LoginSecurityConfig)).(*system.LoginSecurityConfig), nil
	}
}

func (r *Runtime) Reload(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reloadLocked(ctx)
}

// Explicit reloads bypass demand-load backoff and run after any in-flight load,
// so a configuration-change notification cannot be swallowed by singleflight.
func (r *Runtime) reloadLocked(ctx context.Context) error {
	cfg, err := r.load(ctx)
	if err == nil {
		err = Validate(cfg)
	}
	if err != nil {
		// A missed enable notification must not leave a disabled snapshot usable
		// indefinitely after its source becomes unavailable.
		r.value.Store(nil)
		r.loadErr = err
		r.retryAt = time.Now().Add(loadRetryDelay)
		return err
	}
	r.value.Store(proto.Clone(cfg).(*system.LoginSecurityConfig))
	r.loadErr = nil
	r.retryAt = time.Time{}
	return nil
}

func (r *Runtime) Notify(ctx context.Context) error {
	return r.client.Publish(ctx, ConfigChannel, "reload").Err()
}

func (r *Runtime) run(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(ReloadInterval)
	defer ticker.Stop()
	messages := r.subscription.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-messages:
			if !ok {
				messages = nil
			}
		case <-ticker.C:
		}
		attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := r.Reload(attempt); err != nil {
			logx.Errorf("reload login security configuration: %v", err)
		}
		cancel()
	}
}

func (r *Runtime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		r.cancel()
		_ = r.subscription.Close()
		<-r.done
		_ = r.client.Close()
	})
}
