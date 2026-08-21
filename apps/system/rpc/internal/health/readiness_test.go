package health

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type databasePingerStub struct {
	err error
}

func (s databasePingerStub) PingContext(context.Context) error {
	return s.err
}

type redisPingerStub struct {
	ok bool
}

func (s redisPingerStub) PingCtx(context.Context) bool {
	return s.ok
}

func TestReadinessCheckSuccess(t *testing.T) {
	checker := NewReadiness(databasePingerStub{}, redisPingerStub{ok: true})
	if err := checker.Check(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadinessCheckReportsAllFailures(t *testing.T) {
	checker := NewReadiness(
		databasePingerStub{err: errors.New("connection refused")},
		redisPingerStub{ok: false},
	)

	err := checker.Check(context.Background())
	if err == nil {
		t.Fatal("expected readiness error")
	}
	if !strings.Contains(err.Error(), "postgres") || !strings.Contains(err.Error(), "redis") {
		t.Fatalf("expected both dependency failures, got: %v", err)
	}
}
