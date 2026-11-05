//go:build integration

package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdInstanceDiscoveryRunsOnlyForLeaderAndTransfersAfterLoss(t *testing.T) {
	f := newEtcdFixture(t)
	prefix := "/test/instance-discovery-leadership"
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseWork := func() { releaseOnce.Do(func() { close(release) }) }
	var firstCalls, secondCalls atomic.Int32
	a, err := New(f.replicaClient(t), triggerServiceStub{}, triggerServiceStub{}, Config{Prefix: prefix},
		WithInstanceRecovery(func(ctx context.Context) error {
			if firstCalls.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release
			} else {
				<-ctx.Done()
			}
			return ctx.Err()
		}))
	if err != nil {
		t.Fatal(err)
	}
	startRuntime(t, a)
	t.Cleanup(releaseWork)
	awaitSignal(t, entered)
	b, err := New(f.replicaClient(t), triggerServiceStub{}, triggerServiceStub{}, Config{Prefix: prefix},
		WithInstanceRecovery(func(ctx context.Context) error {
			secondCalls.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}))
	if err != nil {
		t.Fatal(err)
	}
	startRuntime(t, b)
	candidateCount := func() int {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := f.client.Get(ctx, makeKeys(prefix).election+"/", clientv3.WithPrefix())
		if err != nil {
			return -1
		}
		return len(response.Kvs)
	}
	waitCondition(t, 10*time.Second, "both stopping discovery replicas to campaign", func() bool { return candidateCount() == 2 })
	if firstCalls.Load() != 1 || secondCalls.Load() != 0 {
		t.Fatal("standby replica started instance discovery")
	}
	revokeLeader(t, f.client, prefix)
	awaitSignal(t, canceled)
	waitCondition(t, 10*time.Second, "successor stopping discovery", func() bool { return secondCalls.Load() == 1 })
	if candidateCount() != 1 || a.Stats().Terms != 1 || firstCalls.Load() != 1 {
		t.Fatal("canceled discovery reentered election before its query returned")
	}
	releaseWork()
	waitCondition(t, 10*time.Second, "cleaned stopping discovery replica to campaign again", func() bool { return candidateCount() == 2 })
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatal("reenrolling standby started another discovery loop")
	}
}
