package health

import (
	"context"
	"errors"
	"fmt"
)

type DatabasePinger interface {
	PingContext(ctx context.Context) error
}

type RedisPinger interface {
	PingCtx(ctx context.Context) bool
}

type Readiness struct {
	database DatabasePinger
	redis    RedisPinger
}

func NewReadiness(database DatabasePinger, redis RedisPinger) *Readiness {
	return &Readiness{
		database: database,
		redis:    redis,
	}
}

func (r *Readiness) Check(ctx context.Context) error {
	var failures []error

	if err := r.database.PingContext(ctx); err != nil {
		failures = append(failures, fmt.Errorf("postgres: %w", err))
	}
	if ok := r.redis.PingCtx(ctx); !ok {
		failures = append(failures, errors.New("redis: ping failed"))
	}

	return errors.Join(failures...)
}
