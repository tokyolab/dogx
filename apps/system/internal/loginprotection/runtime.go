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
	"google.golang.org/protobuf/proto"
)

const ConfigChannel = "dogx:security:config:changed"
const ReloadInterval = 30 * time.Second

type Runtime struct {
	load         func(context.Context) (*system.LoginSecurityConfig, error)
	value        atomic.Pointer[system.LoginSecurityConfig]
	mu           sync.Mutex
	client       redisv9.UniversalClient
	subscription *redisv9.PubSub
	cancel       context.CancelFunc
	done         chan struct{}
	closeOnce    sync.Once
}

// NewRuntime subscribes before loading so an update during startup is not lost.
// Payloads are only invalidation signals; the authoritative value comes from RPC/DB.
func NewRuntime(conf zeroredis.RedisConf, load func(context.Context) (*system.LoginSecurityConfig, error)) (*Runtime, error) {
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
	r := &Runtime{load: load, client: client, cancel: cancel, done: make(chan struct{})}
	r.subscription = client.Subscribe(ctx, ConfigChannel)
	initial, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if _, err := r.subscription.Receive(initial); err != nil {
		cancel()
		_ = r.subscription.Close()
		_ = client.Close()
		return nil, err
	}
	if err := r.Reload(initial); err != nil {
		cancel()
		_ = r.subscription.Close()
		_ = client.Close()
		return nil, err
	}
	go r.run(ctx)
	return r, nil
}

func (r *Runtime) Current() (*system.LoginSecurityConfig, error) {
	cfg := r.value.Load()
	if cfg == nil {
		return nil, errors.New("login protection configuration unavailable")
	}
	return proto.Clone(cfg).(*system.LoginSecurityConfig), nil
}

func (r *Runtime) Reload(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cfg, err := r.load(ctx)
	if err == nil {
		err = Validate(cfg)
	}
	if err != nil {
		// A missed enable notification must not leave a disabled snapshot usable
		// indefinitely after its source becomes unavailable.
		r.value.Store(nil)
		return err
	}
	r.value.Store(proto.Clone(cfg).(*system.LoginSecurityConfig))
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
