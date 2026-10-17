package scheduler

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// electionSession owns the lease for one candidacy and its possible leader term.
// Runtime calls campaign and close sequentially while it may also watch done.
// close must not run concurrently with campaign.
type electionSession interface {
	// campaign waits for election, cancellation, or an error. A nil result must
	// still be checked against done and the context: election may race with
	// session loss. Cancellation initiates exit, but library cleanup may outlive
	// the supplied context's deadline.
	campaign(context.Context) error
	// done returns the same channel on each call. It closes when keepalive ends,
	// the lease expires, or cleanup orphans the session. Closure requires this
	// term to stop; it does not prove the server has removed the lease or entry.
	done() <-chan struct{}
	// close stops and waits for keepalive, then attempts to resign and revoke
	// the lease, joining cleanup errors. Runtime calls it once after campaign
	// and term work have exited. It does not close the shared etcd client.
	// The context bounds remote requests, not the entire method's return time;
	// failed cleanup may leave remote records until the lease expires.
	close(context.Context) error
}

// coordination provides election and configuration hints, not business storage.
// Its methods must be safe for concurrent use. A successful hint write does not
// mean that the leader has loaded the configuration.
type coordination interface {
	// open creates a renewing session. The int is its TTL in seconds; the string
	// is the election key prefix. The context controls the whole session, not
	// just creation. Success returns a non-nil session but does not elect it.
	// The caller must watch done and eventually call close.
	open(context.Context, int, string) (electionSession, error)
	// revision returns the read response's global header revision, not the
	// key's ModRevision. An absent key still yields a valid snapshot revision;
	// the caller starts the subsequent watch at that revision plus one.
	revision(context.Context, string) (int64, error)
	// watch observes one key, not a prefix, starting at the inclusive revision.
	// The caller must cancel the context when done and handle channel closure,
	// Canceled, and Err responses. The etcd client handles recoverable transport
	// reconnects; a disconnected transport alone does not end the session.
	watch(context.Context, string, int64) clientv3.WatchChan
	// put writes a key and value without a lease. It is a configuration-reload
	// hint, not a durable message delivery mechanism; nil does not acknowledge
	// consumption of the hint.
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
