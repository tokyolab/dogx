package scheduler

import (
	"context"
	"errors"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type coordinationStub struct {
	openFn     func(context.Context, int, string) (electionSession, error)
	revisionFn func(context.Context, string) (int64, error)
	watchFn    func(context.Context, string, int64) clientv3.WatchChan
	putFn      func(context.Context, string, string) error
}

func (s coordinationStub) open(ctx context.Context, ttl int, prefix string) (electionSession, error) {
	if s.openFn != nil {
		return s.openFn(ctx, ttl, prefix)
	}
	return nil, errors.New("session unavailable")
}

func (s coordinationStub) revision(ctx context.Context, key string) (int64, error) {
	if s.revisionFn != nil {
		return s.revisionFn(ctx, key)
	}
	return 1, nil
}

func (s coordinationStub) watch(ctx context.Context, key string, revision int64) clientv3.WatchChan {
	if s.watchFn != nil {
		return s.watchFn(ctx, key, revision)
	}
	return nil
}

func (s coordinationStub) put(ctx context.Context, key, value string) error {
	if s.putFn != nil {
		return s.putFn(ctx, key, value)
	}
	return nil
}

type electionSessionStub struct {
	campaignFn func(context.Context) error
	closeFn    func(context.Context) error
	ended      chan struct{}
}

func (s electionSessionStub) campaign(ctx context.Context) error { return s.campaignFn(ctx) }
func (s electionSessionStub) done() <-chan struct{}              { return s.ended }
func (s electionSessionStub) close(ctx context.Context) error {
	if s.closeFn != nil {
		return s.closeFn(ctx)
	}
	return nil
}
