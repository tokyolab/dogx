//go:build integration

package svc

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// systemServiceEtcdEndpoint starts a test-owned etcd rather than reading private
// service configuration. Cleanup waits for the process before removing its data.
func systemServiceEtcdEndpoint(t testing.TB) string {
	t.Helper()
	binary := os.Getenv("DOGX_TEST_ETCD_BINARY")
	if binary == "" {
		t.Fatal("DOGX_TEST_ETCD_BINARY is required for isolated etcd integration tests")
	}
	resolved, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(resolved); err != nil || info.IsDir() {
		t.Fatalf("invalid DOGX_TEST_ETCD_BINARY: %v", err)
	}
	clientAddress := systemServiceTestAddress(t)
	peerAddress := systemServiceTestAddress(t)
	for peerAddress == clientAddress {
		peerAddress = systemServiceTestAddress(t)
	}
	endpoint, peerURL := "http://"+clientAddress, "http://"+peerAddress
	dir := t.TempDir()
	log, err := os.OpenFile(filepath.Join(dir, "etcd.log"), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	command := exec.CommandContext(ctx, resolved, "--name", "test", "--data-dir", filepath.Join(dir, "data"),
		"--listen-client-urls", endpoint, "--advertise-client-urls", endpoint,
		"--listen-peer-urls", peerURL, "--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", "test="+peerURL, "--initial-cluster-token", filepath.Base(dir), "--log-level", "error")
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		cancel()
		t.Fatalf("start isolated etcd: %v", err)
	}
	done := make(chan struct{})
	go func() { _ = command.Wait(); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("isolated etcd did not exit after test cleanup")
		}
	})
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ready, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		probe, stopProbe := context.WithTimeout(ready, 200*time.Millisecond)
		response, err := client.Status(probe, endpoint)
		stopProbe()
		if err == nil && response.Leader != 0 {
			return endpoint
		}
		select {
		case <-done:
			t.Fatal("isolated etcd exited before becoming ready")
		case <-ready.Done():
			t.Fatal("isolated etcd did not become ready within its startup budget")
		case <-ticker.C:
		}
	}
}

func systemServiceTestAddress(t testing.TB) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
