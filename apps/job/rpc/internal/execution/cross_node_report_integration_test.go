//go:build integration

package execution_test

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/execution"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/logic"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestCrossNodeReportCompletesAfterDispatchHostCloses(t *testing.T) {
	if os.Getenv("DOGX_CROSS_NODE_REPORT_TEST") == "1" {
		testCrossNodeReport(t, os.Getenv("DOGX_CROSS_NODE_REPORT_ETCD"))
		return
	}
	// Keep process-global zrpc services isolated. The two production hosts have
	// independent pools, caches and queues; this test closes A, not the OS process.
	endpoint := dispatchHostEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrossNodeReportCompletesAfterDispatchHostCloses$", "-test.v")
	command.Env = append(os.Environ(), "DOGX_CROSS_NODE_REPORT_TEST=1", "DOGX_CROSS_NODE_REPORT_ETCD="+endpoint)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("cross-node report subprocess: %v\n%s", err, output)
	}
	t.Log("A dispatched over discovered TCP RPC and closed; follower B authenticated and persisted the executor report, then completed the parent independently")
}

func testCrossNodeReport(t *testing.T, endpoint string) {
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatal("the parent-owned isolated etcd endpoint is required")
	}
	t.Cleanup(func() { _ = logx.Close() })
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	c, applicationName := dispatchHostConfig(t, sqlDB, endpoint)
	c.Dispatch.ScanInterval = time.Hour
	group := model.ExecutorGroup{Name: "cross-node-workers", DiscoveryKey: c.Scheduler.Prefix + "/executor", Token: "00000000000040008000000000000001"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	definition := model.JobDefinition{Name: "cross-node result", ExecutorGroupID: group.ID, ProcessorName: "test.cross-node",
		ExecutionMode: model.ExecutionModeNormal, Params: "frozen cross-node input", CronExpr: "0 * * * * *",
		Status: basemodel.RecordStatusDisabled, BlockPolicy: model.BlockParallel, RetryIntervalSeconds: 10, TimeoutSeconds: 300}
	if err := db.Create(&definition).Error; err != nil {
		t.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	session, err := concurrency.NewSession(client, concurrency.WithTTL(15))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	leader := concurrency.NewElection(session, c.Scheduler.Prefix+"/scheduler/election")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.Campaign(ctx, "test-owned-leader"); err != nil {
		t.Fatal(err)
	}
	newHost := func(name string) *svc.ServiceContext {
		t.Setenv("PGAPPNAME", name)
		host, err := svc.NewServiceContext(c)
		if err != nil {
			t.Fatalf("initialize cross-node host: %v", err)
		}
		t.Cleanup(func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = host.Drain(shutdown)
			if err := host.Wait(shutdown); err != nil {
				t.Errorf("wait for cross-node host: %v", err)
				return
			}
			if err := host.Close(); err != nil {
				t.Errorf("close cross-node host: %v", err)
			}
		})
		// Observe each independent initial scan before creating work. With the
		// next scan an hour away, only A's production trigger hint can dispatch.
		dispatchHostEventually(t, "initial empty cross-node scan", func(ctx context.Context) bool {
			var done bool
			if err := sqlDB.QueryRowContext(ctx, `SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity WHERE application_name = $1 AND state = 'idle'
				AND (query LIKE '%job_instance%ORDER BY id DESC%' OR query LIKE '%job_task%next_dispatch_at%'))`, name).Scan(&done); err != nil {
				t.Fatal(err)
			}
			return done
		})
		return host
	}
	hostA := newHost(applicationName + "-a")
	hostB := newHost(applicationName + "-b")
	resultConfig := completionHostServerConfig(t, endpoint, c.Scheduler.Prefix+"/results")
	resultServer, err := execution.NewServer(resultConfig, func() *execution.Receiver { return hostB.ResultReceiver })
	if err != nil {
		t.Fatal(err)
	}
	var acceptedReports atomic.Int32
	resultServer.AddUnaryInterceptors(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, err := handler(ctx, request)
		if info.FullMethod == protocol.ResultReceiver_ReportTaskResult_FullMethodName && err == nil {
			acceptedReports.Add(1)
		}
		return response, err
	})
	logx.SetWriter(logx.NewWriter(io.Discard))
	go resultServer.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := resultServer.Shutdown(ctx); err != nil {
			t.Errorf("stop node B result server: %v", err)
		}
	})
	completionHostDiscovery(t, client, resultConfig)
	var reportConfig zrpc.RpcClientConf
	if err := conf.FillDefault(&reportConfig); err != nil {
		t.Fatal(err)
	}
	reportConfig.Etcd = resultConfig.Etcd
	reportClient, err := jobexecutor.NewReportClient(reportConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reportClient.Conn().Close() })
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readyCancel()
	if _, err := grpc_health_v1.NewHealthClient(reportClient.Conn()).Check(readyCtx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("wait for node B discovered result server: %v", err)
	}
	processor := &crossNodeReportProcessor{started: make(chan jobexecutor.Input, 1), release: make(chan struct{})}
	registry := jobexecutor.NewRegistry()
	if err := registry.Register(definition.ProcessorName, processor); err != nil {
		t.Fatal(err)
	}
	executorID := uuid.New()
	runtime, err := jobexecutor.NewRuntime(registry, protocol.NewResultReceiverClient(reportClient.Conn()), group.Name, group.Token, executorID.String(),
		jobexecutor.RuntimeConfig{Concurrency: 1, QueueCapacity: 1, ReportCapacity: 1, ReportConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Drain(ctx); err != nil {
			t.Errorf("drain cross-node executor: %v", err)
		}
		// A drain deadline requests cancellation but does not prove that workers
		// have returned. Keep the report connection and receiver alive until the
		// actual barrier completes; this second deadline only bounds diagnostics.
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer waitCancel()
		if err := runtime.Wait(waitCtx); err != nil {
			t.Errorf("cross-node executor still exiting after drain: %v", err)
			_ = runtime.Wait(context.Background())
		}
	})
	if err := runtime.Start(); err != nil {
		t.Fatal(err)
	}
	auth, err := jobexecutor.NewAuthenticator(group.Name, group.Token)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := jobexecutor.NewRPCAdapter(auth, registry, executorID.String(), runtime)
	if err != nil {
		t.Fatal(err)
	}
	executorConfig := completionHostServerConfig(t, endpoint, group.DiscoveryKey)
	executorConfig.ServiceConf = resultConfig.ServiceConf
	executorServer, err := jobexecutor.NewServer(executorConfig, func() *jobexecutor.RPCAdapter { return adapter })
	if err != nil {
		t.Fatal(err)
	}
	var dispatches atomic.Int32
	executorServer.AddUnaryInterceptors(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == protocol.Executor_DispatchTask_FullMethodName {
			dispatches.Add(1)
		}
		return handler(ctx, request)
	})
	go executorServer.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := executorServer.Shutdown(ctx); err != nil {
			t.Errorf("stop cross-node executor server: %v", err)
		}
	})
	completionHostDiscovery(t, client, executorConfig)
	triggerCtx, triggerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer triggerCancel()
	target, err := hostA.ExecutorClients.Executor(triggerCtx, group.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Client.CancelInstance(triggerCtx, &protocol.CancelInstanceRequest{
		Auth: &protocol.GroupAuth{GroupName: group.Name, Token: group.Token}, InstanceId: 1, Reason: protocol.CancelReason_MANUAL_STOP,
	}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("wait for A's discovered executor connection: %v", err)
	}
	override := false
	created, err := logic.NewTriggerJobLogic(triggerCtx, hostA).TriggerJob(&job.TriggerJobRequest{
		Id: definition.ID, RequestId: uuid.NewString(), ParamsOverridden: &override, OperatorId: 42,
	})
	if err != nil {
		t.Fatalf("trigger on node A: %v", err)
	}
	var input jobexecutor.Input
	select {
	case input = <-processor.started:
	case <-triggerCtx.Done():
		t.Fatal("A did not dispatch business execution")
	}
	if input.JobID != definition.ID || input.InstanceID != created.InstanceId || input.Params != definition.Params {
		t.Fatal("cross-node execution changed the frozen input")
	}
	var bound model.JobTask
	dispatchHostEventually(t, "A's persisted executor ACK binding", func(ctx context.Context) bool {
		if err := db.WithContext(ctx).First(&bound, input.TaskID).Error; err != nil {
			t.Fatal(err)
		}
		return bound.ExecutorInstanceID != nil && bound.ExecutorAddress != nil
	})
	if bound.Status != model.TaskRunning || *bound.ExecutorInstanceID != executorID || *bound.ExecutorAddress != executorConfig.ListenOn || bound.AttemptNo != 1 || bound.UsedAttempts != 1 {
		t.Fatal("A did not persist the expected attempt and binding")
	}

	// Authentication is checked by B's real Receiver before its queue accepts
	// anything. The business remains blocked, so no valid result can race this.
	badReport := &protocol.ReportTaskResultRequest{
		Auth:  &protocol.GroupAuth{GroupName: group.Name, Token: "00000000000040008000000000000002"},
		JobId: definition.ID, InstanceId: input.InstanceID, TaskId: input.TaskID, AttemptNo: 1,
		ExecutorInstanceId: executorID.String(), Outcome: protocol.TaskOutcome_SUCCESS,
	}
	authCtx, authCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, authErr := protocol.NewResultReceiverClient(reportClient.Conn()).ReportTaskResult(authCtx, badReport)
	authCancel()
	if status.Code(authErr) != codes.Unauthenticated {
		t.Fatalf("node B did not reject an invalid group token: code=%s", status.Code(authErr))
	}
	if acceptedReports.Load() != 0 {
		t.Fatal("node B admitted a result before authenticated business completion")
	}
	assertDispatchHostFollower(t, leader)
	shutdown, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := hostA.Drain(shutdown); err != nil {
		t.Fatalf("drain dispatch node A: %v", err)
	}
	if err := hostA.Wait(shutdown); err != nil {
		t.Fatalf("wait for dispatch node A: %v", err)
	}
	if err := hostA.Close(); err != nil {
		t.Fatalf("close dispatch node A: %v", err)
	}
	if _, err := hostA.JobRepo.Get(shutdown, definition.ID); err == nil {
		t.Fatal("node A retained database access after closing")
	}
	dispatchHostEventually(t, "node A's database connections to close", func(ctx context.Context) bool {
		var count int
		if err := sqlDB.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", applicationName+"-a").Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count == 0
	})
	// Only the executor and B remain active. No repository write, explicit
	// completion hint or leadership change is used to finish the execution.
	close(processor.release)
	var parent model.JobInstance
	dispatchHostEventually(t, "B's result persistence and automatic parent completion", func(ctx context.Context) bool {
		if err := db.WithContext(ctx).First(&parent, input.InstanceID).Error; err != nil {
			t.Fatal(err)
		}
		return parent.Status == model.InstanceSucceeded
	})
	var tasks []model.JobTask
	if err := db.Where("instance_id = ?", input.InstanceID).Find(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("cross-node completion created %d Tasks", len(tasks))
	}
	saved := tasks[0]
	if saved.ID != input.TaskID || saved.Status != model.TaskSucceeded || saved.AttemptNo != 1 || saved.UsedAttempts != 1 ||
		saved.ExecutorInstanceID == nil || *saved.ExecutorInstanceID != executorID || saved.ExecutorAddress == nil || *saved.ExecutorAddress != executorConfig.ListenOn ||
		saved.FinishedAt == nil || parent.FinishedAt == nil || saved.StartedAt == nil || bound.StartedAt == nil || !saved.StartedAt.Equal(*bound.StartedAt) ||
		saved.ResultSummary == nil || *saved.ResultSummary != "cross-node business completed" || parent.ResultSummary == nil || *parent.ResultSummary != *saved.ResultSummary ||
		saved.EndReason == nil || *saved.EndReason != model.EndReasonSuccess || parent.EndReason == nil || *parent.EndReason != model.EndReasonSuccess ||
		string(saved.ResultData) != "9007199254740993" {
		t.Fatal("B did not preserve the original attempt, binding and exact terminal result")
	}
	if dispatches.Load() != 1 || processor.calls.Load() != 1 || acceptedReports.Load() < 1 {
		t.Fatalf("unexpected cross-node calls: dispatch=%d business=%d accepted reports=%d", dispatches.Load(), processor.calls.Load(), acceptedReports.Load())
	}
	assertDispatchHostFollower(t, leader)
}

// crossNodeReportProcessor holds business completion until dispatch host A has
// released every component and connection. Reporting remains the real Runtime's
// responsibility and targets B through its discovered TCP RPC connection.
type crossNodeReportProcessor struct {
	started chan jobexecutor.Input
	release chan struct{}
	calls   atomic.Int32
}

func (p *crossNodeReportProcessor) Execute(ctx context.Context, input jobexecutor.Input) (jobexecutor.Result, error) {
	p.calls.Add(1)
	select {
	case p.started <- input:
	case <-ctx.Done():
		return jobexecutor.Result{}, ctx.Err()
	}
	select {
	case <-p.release:
		return jobexecutor.Result{Data: []byte("9007199254740993"), Summary: "cross-node business completed"}, nil
	case <-ctx.Done():
		return jobexecutor.Result{}, ctx.Err()
	}
}
