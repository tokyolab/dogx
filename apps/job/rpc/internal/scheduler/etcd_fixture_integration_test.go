//go:build integration

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdFixture struct {
	binary, data, clientURL, peerURL, logPath string
	client                                    *clientv3.Client
	command                                   *exec.Cmd
	finished                                  chan error
	log                                       *os.File
}

func newEtcdFixture(t *testing.T) *etcdFixture {
	t.Helper()
	binary := os.Getenv("DOGX_TEST_ETCD_BINARY")
	if binary == "" {
		t.Fatal("DOGX_TEST_ETCD_BINARY is required; tests start their own isolated etcd process")
	}
	resolved, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(resolved); err != nil || info.IsDir() {
		t.Fatalf("invalid DOGX_TEST_ETCD_BINARY: %v", err)
	}
	// Reserve both local ports together to prevent choosing the same ephemeral
	// port twice. Release immediately before etcd binds its own listeners.
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = clientListener.Close() }()
	peerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peerListener.Close() }()
	dir := t.TempDir()
	f := &etcdFixture{binary: resolved, data: filepath.Join(dir, "data"),
		clientURL: "http://" + clientListener.Addr().String(), peerURL: "http://" + peerListener.Addr().String(),
		logPath: filepath.Join(dir, "etcd.log")}
	t.Cleanup(func() { f.stop(t) })
	f.client, err = clientv3.New(clientv3.Config{Endpoints: []string{f.clientURL}, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.client.Close() })
	_ = clientListener.Close()
	_ = peerListener.Close()
	f.start(t)
	return f
}

func (f *etcdFixture) start(t *testing.T) {
	t.Helper()
	var err error
	f.log, err = os.OpenFile(f.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.command = exec.Command(f.binary, "--name", "test", "--data-dir", f.data,
		"--listen-client-urls", f.clientURL, "--advertise-client-urls", f.clientURL,
		"--listen-peer-urls", f.peerURL, "--initial-advertise-peer-urls", f.peerURL,
		"--initial-cluster", "test="+f.peerURL, "--initial-cluster-token", filepath.Base(filepath.Dir(f.data)),
		"--log-level", "error")
	f.command.Stdout, f.command.Stderr = f.log, f.log
	if err := f.command.Start(); err != nil {
		_ = f.log.Close()
		t.Fatalf("start etcd: %v", err)
	}
	f.finished = make(chan error, 1)
	command, finished := f.command, f.finished
	go func() { finished <- command.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		probeCtx, probeCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		status, probeErr := f.client.Status(probeCtx, f.clientURL)
		probeCancel()
		if probeErr == nil && status.Leader != 0 {
			return
		}
		select {
		case err := <-f.finished:
			f.finished = nil
			logs, _ := os.ReadFile(f.logPath)
			t.Fatalf("etcd exited before ready: %v\n%s", err, logs)
		case <-ctx.Done():
			logs, _ := os.ReadFile(f.logPath)
			t.Fatalf("etcd did not become ready: %v\n%s", probeErr, logs)
		case <-ticker.C:
		}
	}
}

func (f *etcdFixture) stop(t *testing.T) {
	t.Helper()
	if f.finished != nil {
		_ = f.command.Process.Kill()
		select {
		case <-f.finished:
		case <-time.After(5 * time.Second):
			t.Error("etcd process did not exit after test cleanup")
		}
		f.finished = nil
	}
	if f.log != nil {
		_ = f.log.Close()
		f.log = nil
	}
}

func (f *etcdFixture) replicaClient(t *testing.T) *clientv3.Client {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{f.clientURL}, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func waitCondition(t *testing.T, budget time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(budget)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

func startRuntime(t *testing.T, r *Runtime) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("scheduler shutdown: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("scheduler did not release its term resources")
		}
	})
	return cancel, done
}

func revokeLeader(t *testing.T, client *clientv3.Client, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := client.Get(ctx, makeKeys(prefix).election+"/", clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend), clientv3.WithLimit(1))
	if err != nil || len(response.Kvs) != 1 {
		t.Fatalf("read elected lease: %v", err)
	}
	lease := clientv3.LeaseID(response.Kvs[0].Lease)
	if lease == 0 {
		t.Fatal("elected key has no lease")
	}
	if _, err := client.Revoke(ctx, lease); err != nil {
		t.Fatalf("revoke elected lease %s: %v", fmt.Sprint(lease), err)
	}
}
