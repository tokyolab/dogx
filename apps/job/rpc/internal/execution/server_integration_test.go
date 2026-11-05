//go:build integration

package execution

import (
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecutionServerEtcdDiscovery(t *testing.T) {
	if os.Getenv("DOGX_EXECUTION_ETCD_TEST") == "1" {
		testExecutionServerEtcdDiscovery(t, os.Getenv("DOGX_EXECUTION_ETCD_TEST_ENDPOINT"))
		return
	}
	// The parent owns etcd cleanup even if the isolated zrpc test process hangs.
	// No test endpoint is read from development configuration or a shared service.
	fixture := newExecutionEtcdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionServerEtcdDiscovery$", "-test.v")
	cmd.Env = append(os.Environ(), "DOGX_EXECUTION_ETCD_TEST=1", "DOGX_EXECUTION_ETCD_TEST_ENDPOINT="+fixture.endpoint)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("etcd discovery subprocess: %v\n%s", err, output)
	}
	t.Log("isolated etcd registration, dispatch/report discovery, peer address, directed cancellation, and missing-key deadline passed")
}

func testExecutionServerEtcdDiscovery(t *testing.T, endpoint string) {
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatal("the parent-owned isolated etcd endpoint is required")
	}
	registry := jobexecutor.NewRegistry()
	if err := registry.Register("test.process", &transportProcessor{}); err != nil {
		t.Fatal(err)
	}
	auth, err := jobexecutor.NewAuthenticator("test-workers", receiverTestToken)
	if err != nil {
		t.Fatal(err)
	}
	admission := &transportAdmission{work: make(chan jobexecutor.Invocation, 1)}
	adapter, err := jobexecutor.NewRPCAdapter(auth, registry, receiverTestID, admission)
	if err != nil {
		t.Fatal(err)
	}
	results := &transportResults{reports: make(chan taskresult.Report, 1)}
	receiver, err := NewReceiver(&transportGroupAuth{}, results)
	if err != nil {
		t.Fatal(err)
	}
	keyPrefix := "/dogx-test/execution/" + uuid.NewString()
	executorConfig, resultConfig := transportServerConfig(t), transportServerConfig(t)
	resultConfig.ServiceConf = executorConfig.ServiceConf
	executorConfig.Etcd = discov.EtcdConf{Hosts: []string{endpoint}, Key: keyPrefix + "/executor"}
	resultConfig.Etcd = discov.EtcdConf{Hosts: []string{endpoint}, Key: keyPrefix + "/results"}
	executorServer, err := jobexecutor.NewServer(executorConfig, func() *jobexecutor.RPCAdapter { return adapter })
	if err != nil {
		t.Fatal(err)
	}
	resultServer, err := NewServer(resultConfig, func() *Receiver { return receiver })
	if err != nil {
		t.Fatal(err)
	}
	logx.SetWriter(logx.NewWriter(io.Discard))
	for _, server := range []*rpchost.Server{executorServer, resultServer} {
		go server.Start()
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, server := range []*rpchost.Server{executorServer, resultServer} {
			if err := server.Shutdown(ctx); err != nil {
				t.Errorf("stop discovered RPC server: %v", err)
			}
		}
		// zrpc owns its publisher through process-wide wrap-up hooks. Closing
		// inbound RPCs alone does not promise immediate etcd deregistration;
		// this process exits before the parent stops its isolated etcd fixture.
		if err := logx.Close(); err != nil {
			t.Errorf("close discovery test logs: %v", err)
		}
	})
	executorConn := executionDiscoveredClient(t, executorConfig.Etcd, false)
	resultConn := executionDiscoveredClient(t, resultConfig.Etcd, true)
	for _, conn := range []*grpc.ClientConn{executorConn, resultConn} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true))
		cancel()
		if err != nil {
			t.Fatalf("health through etcd discovery: %v", err)
		}
	}
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = etcdClient.Close() })
	for _, config := range []zrpc.RpcServerConf{executorConfig, resultConfig} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		registration, err := etcdClient.Get(ctx, config.Etcd.Key+"/", clientv3.WithPrefix())
		cancel()
		if err != nil {
			t.Fatalf("read test service registration: %v", err)
		}
		if len(registration.Kvs) != 1 || string(registration.Kvs[0].Value) != config.ListenOn || registration.Kvs[0].Lease == 0 {
			t.Fatal("service did not publish its listening address under a leased discovery key")
		}
	}
	request := &protocol.DispatchTaskRequest{
		Auth:  &protocol.GroupAuth{GroupName: "test-workers", Token: receiverTestToken},
		JobId: 11, InstanceId: 22, TaskId: 33, AttemptNo: 4, ProcessorName: "test.process",
		DeadlineAt: timestamppb.New(time.Now().Add(time.Minute)),
		Input:      &protocol.DispatchTaskRequest_Ordinary{Ordinary: &protocol.OrdinaryInput{}},
	}
	var executorPeer peer.Peer
	ack, err := protocol.NewExecutorClient(executorConn).DispatchTask(context.Background(), request, grpc.Peer(&executorPeer))
	if err != nil || ack.GetStatus() != protocol.DispatchStatus_ACCEPTED || ack.GetExecutorInstanceId() != receiverTestID {
		t.Fatalf("discovered dispatch did not acknowledge admission: %v", err)
	}
	if executorPeer.Addr == nil || executorPeer.Addr.String() != executorConfig.ListenOn {
		t.Fatal("dispatch peer did not identify the selected executor listening address")
	}
	// Directed cancellation uses the selected peer, not another discovery lookup.
	directedConn := transportClient(t, executorPeer.Addr.String(), false)
	cancelCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = protocol.NewExecutorClient(directedConn).CancelInstance(cancelCtx, &protocol.CancelInstanceRequest{
		Auth: request.Auth, InstanceId: request.InstanceId, Reason: protocol.CancelReason_MANUAL_STOP,
	})
	cancel()
	if err != nil || admission.cancels.Load() != 1 {
		t.Fatalf("directed cancellation to discovered peer: %v", err)
	}
	var resultPeer peer.Peer
	if _, err := protocol.NewResultReceiverClient(resultConn).ReportTaskResult(context.Background(), validReceiverRequest(), grpc.Peer(&resultPeer)); err != nil {
		t.Fatalf("report through separate discovery key: %v", err)
	}
	if resultPeer.Addr == nil || resultPeer.Addr.String() != resultConfig.ListenOn {
		t.Fatal("report did not reach the independently discovered result listener")
	}
	retained := <-results.reports
	if retained.GroupID != 41 || retained.TaskID != request.TaskId || retained.AttemptNo != request.AttemptNo {
		t.Fatal("discovered result receiver lost trusted or execution identity")
	}
	missing := executionDiscoveredClient(t, discov.EtcdConf{Hosts: []string{endpoint}, Key: keyPrefix + "/absent"}, false)
	missingCtx, missingCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer missingCancel()
	started := time.Now()
	response, err := protocol.NewExecutorClient(missing).DispatchTask(missingCtx, request, grpc.WaitForReady(true))
	if response != nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("missing discovery key must not produce an admission ACK: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("missing-key call exceeded its caller deadline")
	}
}

func executionDiscoveredClient(t *testing.T, target discov.EtcdConf, report bool) *grpc.ClientConn {
	t.Helper()
	var config zrpc.RpcClientConf
	if err := conf.FillDefault(&config); err != nil {
		t.Fatal(err)
	}
	config.Etcd = target
	config.NonBlock = true
	var client zrpc.Client
	var err error
	if report {
		client, err = jobexecutor.NewReportClient(config)
	} else {
		client, err = NewExecutorClient(config, DefaultDispatchRPCTimeout)
	}
	if err != nil {
		t.Fatalf("create discovery client: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Conn().Close(); err != nil {
			t.Errorf("close discovery client: %v", err)
		}
	})
	return client.Conn()
}

// executionEtcdFixture owns one test-only process; its lifetime exceeds the
// isolated RPC subprocess so failures cannot leave a running etcd behind.
type executionEtcdFixture struct {
	endpoint string
	command  *exec.Cmd
	finished chan error
	log      *os.File
}

func newExecutionEtcdFixture(t *testing.T) *executionEtcdFixture {
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
	fixture := &executionEtcdFixture{endpoint: "http://" + clientListener.Addr().String()}
	peerURL := "http://" + peerListener.Addr().String()
	fixture.log, err = os.OpenFile(filepath.Join(dir, "etcd.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fixture.stop(t) })
	fixture.command = exec.Command(resolved, "--name", "test", "--data-dir", filepath.Join(dir, "data"),
		"--listen-client-urls", fixture.endpoint, "--advertise-client-urls", fixture.endpoint,
		"--listen-peer-urls", peerURL, "--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", "test="+peerURL, "--initial-cluster-token", filepath.Base(dir),
		"--log-level", "error")
	fixture.command.Stdout, fixture.command.Stderr = fixture.log, fixture.log
	_ = clientListener.Close()
	_ = peerListener.Close()
	if err := fixture.command.Start(); err != nil {
		t.Fatalf("start isolated etcd: %v", err)
	}
	fixture.finished = make(chan error, 1)
	go func() { fixture.finished <- fixture.command.Wait() }()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{fixture.endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		probeCtx, probeCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		response, probeErr := client.Status(probeCtx, fixture.endpoint)
		probeCancel()
		if probeErr == nil && response.Leader != 0 {
			return fixture
		}
		select {
		case <-fixture.finished:
			fixture.finished = nil
			t.Fatal("isolated etcd exited before becoming ready")
		case <-ctx.Done():
			t.Fatal("isolated etcd did not become ready within its startup budget")
		case <-ticker.C:
		}
	}
}

func (f *executionEtcdFixture) stop(t *testing.T) {
	t.Helper()
	if f.finished != nil {
		_ = f.command.Process.Kill()
		select {
		case <-f.finished:
		case <-time.After(5 * time.Second):
			t.Error("isolated etcd process did not exit after test cleanup")
		}
		f.finished = nil
	}
	if f.log != nil {
		_ = f.log.Close()
	}
}
