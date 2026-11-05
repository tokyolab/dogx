//go:build integration && e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestJobProcessesRecoverAfterEtcdRestartEndToEnd keeps the production Job host
// and the System executor host alive while their shared test etcd is stopped.
// The pinned go-zero Publisher and etcd concurrency.Session replace expired
// keepalive sessions with new leases after the same data directory restarts;
// this is not a universal etcd recovery requirement. Persisted old keys alone
// are insufficient evidence of recovery for these clients.
// A new ordinary Task then runs through discovery and reports its real business
// result. Existing RPC connections need not break when etcd becomes unavailable;
// this does not simulate an asymmetric partition or lost etcd storage.
func TestJobProcessesRecoverAfterEtcdRestartEndToEnd(t *testing.T) {
	etcd := newJobRestartableEtcd(t)
	fixture := newJobLifecycleFixture(t, etcd.endpoint)
	node := fixture.startJob(t, "etcd-recovery-job")
	fixture.createGroup(t, node)
	executor := fixture.startSystem(t, "etcd-recovery-system", true)
	processes := []*runningProcess{node.process, executor.process}

	// Complete a baseline round before the outage. Its binding records the
	// executor's process identity, which must remain unchanged after recovery.
	jobID, instanceID := fixture.trigger(t, node, "gated")
	before := fixture.waitRunning(t, node, instanceID, processes...)
	fixture.releaseBusiness(t, before.Id)
	fixture.waitSuccess(t, node, jobID, instanceID, before.Id, processes...)
	registrations := []jobEtcdRegistration{
		{prefix: fixture.prefix + "/management/", value: fmt.Sprintf("127.0.0.1:%d", node.managementPort)},
		{prefix: fixture.prefix + "/results/", value: fmt.Sprintf("127.0.0.1:%d", node.resultPort)},
		{prefix: fixture.prefix + "/executor/", value: fmt.Sprintf("127.0.0.1:%d", executor.executorPort)},
		{prefix: fixture.prefix + "/scheduler/election/", value: "scheduler", election: true},
	}
	oldLeases := make(map[int64]bool)
	jobE2EEventually(t, processes, "leased discovery and scheduler election before the outage", func() bool {
		return jobEtcdRegistrationsReady(t, context.Background(), fixture, registrations, nil, oldLeases)
	})
	var longestTTL int64
	for lease := range oldLeases {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		response, err := fixture.discovery.TimeToLive(ctx, clientv3.LeaseID(lease))
		cancel()
		if err != nil || response.GrantedTTL <= 0 || response.GrantedTTL > 30 {
			t.Fatal("test registration has an unavailable or unexpected lease lifetime")
		}
		longestTTL = max(longestTTL, response.GrantedTTL)
	}
	// Wait beyond the observed granted lifetimes so client keepalive sessions
	// really fail. Server-side lease time may pause while etcd is stopped; the
	// new-lease assertions below, not elapsed time, prove client re-registration.
	outage := time.Duration(longestTTL)*time.Second + 3*time.Second
	etcd.stop(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, err := fixture.discovery.Get(ctx, fixture.prefix, clientv3.WithPrefix())
	cancel()
	if err == nil {
		t.Fatal("discovery read succeeded after the isolated etcd process exited")
	}
	timer := time.NewTimer(outage)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for waiting := true; waiting; {
		assertJobEtcdProcessesAlive(t, processes)
		select {
		case <-timer.C:
			waiting = false
		case <-ticker.C:
		}
	}
	ticker.Stop()
	timer.Stop()
	// All four listeners must still serve in their original OS processes. This
	// checks availability without claiming new scheduling works during an outage.
	lifecycleWaitHealth(t, lifecycleConnection(t, node.managementPort), node.process)
	lifecycleWaitHealth(t, lifecycleConnection(t, node.resultPort), node.process)
	lifecycleWaitHealth(t, lifecycleConnection(t, executor.managementPort), executor.process)
	lifecycleWaitHealth(t, lifecycleConnection(t, executor.executorPort), executor.process)

	etcd.start(t)
	waitJobEtcdRecovery(t, fixture, registrations, oldLeases, processes)
	newJobID, newInstanceID := fixture.trigger(t, node, "gated")
	after := fixture.waitRunning(t, node, newInstanceID, processes...)
	if newJobID == jobID || newInstanceID == instanceID || after.Id == before.Id ||
		after.GetExecutorInstanceId() != before.GetExecutorInstanceId() ||
		after.GetExecutorAddress() != before.GetExecutorAddress() {
		t.Fatal("post-recovery execution did not use a new Task in the original executor process")
	}
	fixture.releaseBusiness(t, after.Id)
	fixture.waitSuccess(t, node, newJobID, newInstanceID, after.Id, processes...)
	assertJobEtcdProcessesAlive(t, processes)
	t.Logf("the original Job and System processes reacquired discovery and election leases after an etcd outage of %s; a new ordinary Task completed once", outage)
}

type jobEtcdRegistration struct {
	prefix   string
	value    string
	election bool
}

// jobEtcdRegistrationsReady reads one bounded test namespace. rejected excludes
// pre-outage leases, so old keys restored from disk cannot satisfy recovery.
// For election, only the smallest CreateRevision identifies the elected node;
// a later candidate waiting behind an old lease does not establish leadership.
// Captured leases are published only after every registration and leader match.
func jobEtcdRegistrationsReady(t testing.TB, parent context.Context, fixture *jobLifecycleFixture, registrations []jobEtcdRegistration, rejected, captured map[int64]bool) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	response, err := fixture.discovery.Get(ctx, fixture.prefix+"/", clientv3.WithPrefix())
	if err != nil {
		return false
	}
	leases := make(map[int64]bool)
	for _, registration := range registrations {
		if registration.election {
			oldest := -1
			for index, entry := range response.Kvs {
				if strings.HasPrefix(string(entry.Key), registration.prefix) &&
					(oldest < 0 || entry.CreateRevision < response.Kvs[oldest].CreateRevision) {
					oldest = index
				}
			}
			if oldest < 0 {
				return false
			}
			leader := response.Kvs[oldest]
			if string(leader.Value) != registration.value || leader.Lease <= 0 || rejected[leader.Lease] {
				return false
			}
			leases[leader.Lease] = true
			continue
		}
		found := false
		for _, entry := range response.Kvs {
			if strings.HasPrefix(string(entry.Key), registration.prefix) && string(entry.Value) == registration.value &&
				entry.Lease > 0 && !rejected[entry.Lease] {
				found = true
				leases[entry.Lease] = true
			}
		}
		if !found {
			return false
		}
	}
	for lease := range leases {
		if captured != nil {
			captured[lease] = true
		}
	}
	return true
}

func waitJobEtcdRecovery(t testing.TB, fixture *jobLifecycleFixture, registrations []jobEtcdRegistration, oldLeases map[int64]bool, processes []*runningProcess) {
	t.Helper()
	// The scheduler may already be in its bounded 30-second election backoff
	// when etcd returns. Give that attempt and discovery recovery one shared,
	// finite budget without changing the other end-to-end observation limits.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		assertJobEtcdProcessesAlive(t, processes)
		if jobEtcdRegistrationsReady(t, ctx, fixture, registrations, oldLeases, nil) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for new discovery leases and the elected scheduler after etcd recovery")
		case <-ticker.C:
		}
	}
}

func assertJobEtcdProcessesAlive(t testing.TB, processes []*runningProcess) {
	t.Helper()
	for _, process := range processes {
		select {
		case <-process.done:
			t.Fatal(process.failure())
		default:
		}
	}
}

// jobRestartableEtcd owns the same data directory and listen addresses across
// two actual OS processes. Cleanup is registered before application processes,
// so the current etcd remains available until those processes have been reaped.
// start and stop are called serially by the test; done publishes the exit error.
type jobRestartableEtcd struct {
	binary, endpoint string
	arguments        []string
	clusterID        uint64
	command          *exec.Cmd
	done             chan struct{}
	exitErr          error
	logs             *synchronizedBuffer
}

func newJobRestartableEtcd(t testing.TB) *jobRestartableEtcd {
	t.Helper()
	endpoint := "http://127.0.0.1:" + strconv.Itoa(freeTCPPort(t))
	peerURL := "http://127.0.0.1:" + strconv.Itoa(freeTCPPort(t))
	for peerURL == endpoint {
		peerURL = "http://127.0.0.1:" + strconv.Itoa(freeTCPPort(t))
	}
	dir := t.TempDir()
	e := &jobRestartableEtcd{binary: requiredBinary(t, "DOGX_TEST_ETCD_BINARY"), endpoint: endpoint}
	e.arguments = []string{
		"--name", "test", "--data-dir", filepath.Join(dir, "data"),
		"--listen-client-urls", endpoint, "--advertise-client-urls", endpoint,
		"--listen-peer-urls", peerURL, "--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", "test=" + peerURL, "--initial-cluster-token", filepath.Base(dir), "--log-level", "error",
	}
	t.Cleanup(func() {
		if e.command == nil {
			return
		}
		select {
		case <-e.done:
		default:
			_ = e.command.Process.Kill()
			select {
			case <-e.done:
			case <-time.After(5 * time.Second):
				t.Error("restarted test etcd did not exit during cleanup")
			}
		}
	})
	e.start(t)
	return e
}

func (e *jobRestartableEtcd) start(t testing.TB) {
	t.Helper()
	if e.command != nil {
		select {
		case <-e.done:
		default:
			t.Fatal("cannot restart test etcd before its previous process exits")
		}
	}
	command := exec.Command(e.binary, e.arguments...)
	configureE2ESignalProcess(command)
	command.Env = os.Environ()
	e.logs = &synchronizedBuffer{}
	command.Stdout, command.Stderr = e.logs, e.logs
	if err := command.Start(); err != nil {
		t.Fatalf("start restartable test etcd: %v", err)
	}
	e.command, e.done, e.exitErr = command, make(chan struct{}), nil
	done := e.done
	go func() { e.exitErr = command.Wait(); close(done) }()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{e.endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	jobE2EEventually(t, nil, "restarted test etcd readiness", func() bool {
		select {
		case <-e.done:
			t.Fatalf("test etcd exited unexpectedly: %v\n%s", e.exitErr, e.logs.String())
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		response, err := client.Status(ctx, e.endpoint)
		if err != nil || response.Leader == 0 {
			return false
		}
		if e.clusterID != 0 && response.Header.ClusterId != e.clusterID {
			t.Fatal("etcd restart changed the persisted cluster identity")
		}
		e.clusterID = response.Header.ClusterId
		return true
	})
}

func (e *jobRestartableEtcd) stop(t testing.TB) {
	t.Helper()
	select {
	case <-e.done:
		t.Fatal("test etcd exited before fault injection")
	default:
	}
	if err := e.command.Process.Kill(); err != nil {
		t.Fatalf("stop isolated test etcd: %v", err)
	}
	select {
	case <-e.done:
		if e.exitErr == nil {
			t.Fatal("killed test etcd reported a clean exit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("isolated test etcd did not exit after Kill")
	}
}
