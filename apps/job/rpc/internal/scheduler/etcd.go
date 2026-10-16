package scheduler

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type electionSession interface {
	campaign(context.Context) error
	done() <-chan struct{}
	close(context.Context) error
}

type coordination interface {
	open(context.Context, int, string) (electionSession, error)
	revision(context.Context, string) (int64, error)
	watch(context.Context, string, int64) clientv3.WatchChan
	put(context.Context, string, string) error
}

type etcdCoordination struct {
	client  *clientv3.Client
	request time.Duration
}

func (e etcdCoordination) open(ctx context.Context, ttl int, prefix string) (electionSession, error) {
	grantCtx, cancel := context.WithTimeout(ctx, e.request)
	lease, err := e.client.Grant(grantCtx, int64(ttl))
	cancel()
	if err != nil {
		return nil, err
	}
	// NewSession otherwise uses the same context for Grant and keepalive. Grant
	// has a short budget; the existing lease keeps the Session on the lifecycle.
	s, err := concurrency.NewSession(e.client, concurrency.WithLease(lease.ID), concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
	if err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), e.request)
		defer cleanupCancel()
		_, _ = e.client.Revoke(cleanupCtx, lease.ID)
		return nil, err
	}
	return &etcdSession{client: e.client, session: s, election: concurrency.NewElection(s, prefix)}, nil
}

func (e etcdCoordination) revision(ctx context.Context, key string) (int64, error) {
	r, err := e.client.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	return r.Header.Revision, nil
}

func (e etcdCoordination) watch(ctx context.Context, key string, revision int64) clientv3.WatchChan {
	return e.client.Watch(ctx, key, clientv3.WithRev(revision))
}

func (e etcdCoordination) put(ctx context.Context, key, value string) error {
	_, err := e.client.Put(ctx, key, value)
	return err
}

type etcdSession struct {
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
}

func (s *etcdSession) campaign(ctx context.Context) error {
	// Campaign may perform its own cancellation cleanup using client.Ctx();
	// that official-library behavior is not bounded by our short request budget.
	return s.election.Campaign(ctx, "scheduler")
}

func (s *etcdSession) done() <-chan struct{} { return s.session.Done() }

func (s *etcdSession) close(ctx context.Context) error {
	s.session.Orphan()
	resignErr := s.election.Resign(ctx)
	_, revokeErr := s.client.Revoke(ctx, s.session.Lease())
	return errors.Join(resignErr, revokeErr)
}
