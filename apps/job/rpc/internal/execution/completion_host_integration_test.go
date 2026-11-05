//go:build integration

package execution_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
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
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestServiceContextCompletesOrdinaryExecutionOnFollower(t *testing.T) {
	if os.Getenv("DOGX_COMPLETION_HOST_TEST") == "1" {
		testCompletionHost(t, os.Getenv("DOGX_COMPLETION_HOST_ETCD"), false)
		return
	}
	// Native zrpc publishers and process-global services are confined to this
	// child. The parent keeps its isolated etcd alive until the child exits.
	endpoint := dispatchHostEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestServiceContextCompletesOrdinaryExecutionOnFollower$", "-test.v")
	command.Env = append(os.Environ(), "DOGX_COMPLETION_HOST_TEST=1", "DOGX_COMPLETION_HOST_ETCD="+endpoint)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("completion host subprocess: %v\n%s", err, output)
	}
	t.Log("production follower completed ordinary success and final failure through etcd dispatch, executor business, result RPC, persistence and automatic parent hints")
}

func TestServiceContextRetriesOrdinaryExecutionOnFollower(t *testing.T) {
	if os.Getenv("DOGX_RETRY_HOST_TEST") == "1" {
		testCompletionHost(t, os.Getenv("DOGX_COMPLETION_HOST_ETCD"), true)
		return
	}
	endpoint := dispatchHostEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestServiceContextRetriesOrdinaryExecutionOnFollower$", "-test.v")
	command.Env = append(os.Environ(), "DOGX_RETRY_HOST_TEST=1", "DOGX_COMPLETION_HOST_ETCD="+endpoint)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("retry host subprocess: %v\n%s", err, output)
	}
	t.Log("production follower retried failed, busy and rejected attempts through the shared dispatch runtime and completed their original Instances")
}

func testCompletionHost(t *testing.T, endpoint string, retryScenarios bool) {
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatal("the parent-owned isolated etcd endpoint is required")
	}
	t.Cleanup(func() { _ = logx.Close() })
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	c, applicationName := dispatchHostConfig(t, sqlDB, endpoint)
	c.Dispatch.ScanInterval = time.Hour
	if retryScenarios {
		c.Dispatch.ScanInterval = 50 * time.Millisecond
	}
	group := model.ExecutorGroup{Name: "completion-host-workers", DiscoveryKey: c.Scheduler.Prefix + "/executor", Token: "00000000000040008000000000000001"}
	if err := db.Create(&group).Error; err != nil {
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
	host, err := svc.NewServiceContext(c)
	if err != nil {
		t.Fatalf("initialize production completion host: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = host.Drain(ctx)
		if err := host.Wait(ctx); err != nil {
			t.Errorf("wait for completion host: %v", err)
			return
		}
		if err := host.Close(); err != nil {
			t.Errorf("close completion host: %v", err)
		}
	})
	// In the first-attempt scenarios the next scan is an hour away, requiring
	// production trigger hints. Retry scenarios use the real periodic scanner.
	dispatchHostEventually(t, "initial empty waiting scan", func(ctx context.Context) bool {
		var done bool
		if err := sqlDB.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity WHERE application_name = $1 AND state = 'idle'
			AND (query LIKE '%job_instance%ORDER BY id DESC%' OR query LIKE '%job_task%next_dispatch_at%'))`, applicationName).Scan(&done); err != nil {
			t.Fatal(err)
		}
		return done
	})
	resultConfig := completionHostServerConfig(t, endpoint, c.Scheduler.Prefix+"/results")
	resultServer, err := execution.NewServer(resultConfig, func() *execution.Receiver { return host.ResultReceiver })
	if err != nil {
		t.Fatal(err)
	}
	var reports atomic.Int32
	resultServer.AddUnaryInterceptors(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == protocol.ResultReceiver_ReportTaskResult_FullMethodName {
			reports.Add(1)
		}
		return handler(ctx, req)
	})
	logx.SetWriter(logx.NewWriter(io.Discard))
	go resultServer.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := resultServer.Shutdown(ctx); err != nil {
			t.Errorf("stop result receiver server: %v", err)
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
		t.Fatalf("wait for discovered result receiver: %v", err)
	}
	processor := &completionHostProcessor{calls: make(map[int64][]jobexecutor.Input)}
	registry := jobexecutor.NewRegistry()
	if err := registry.Register("test.completion", processor); err != nil {
		t.Fatal(err)
	}
	executorID := uuid.NewString()
	runtime, err := jobexecutor.NewRuntime(registry, protocol.NewResultReceiverClient(reportClient.Conn()), group.Name, group.Token, executorID,
		jobexecutor.RuntimeConfig{Concurrency: 2, QueueCapacity: 4, ReportCapacity: 4, ReportConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Drain(ctx); err != nil {
			t.Errorf("drain completion executor: %v", err)
		}
	})
	if err := runtime.Start(); err != nil {
		t.Fatal(err)
	}
	auth, err := jobexecutor.NewAuthenticator(group.Name, group.Token)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := jobexecutor.NewRPCAdapter(auth, registry, executorID, runtime)
	if err != nil {
		t.Fatal(err)
	}
	executorConfig := completionHostServerConfig(t, endpoint, group.DiscoveryKey)
	// Both servers share exactly the same process service settings.
	executorConfig.ServiceConf = resultConfig.ServiceConf
	executorServer, err := jobexecutor.NewServer(executorConfig, func() *jobexecutor.RPCAdapter { return adapter })
	if err != nil {
		t.Fatal(err)
	}
	var dispatches atomic.Int32
	var dispatched completionHostDispatches
	executorServer.AddUnaryInterceptors(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == protocol.Executor_DispatchTask_FullMethodName {
			dispatches.Add(1)
			request := req.(*protocol.DispatchTaskRequest)
			dispatched.record(request)
			if retryScenarios && request.AttemptNo == 1 {
				// Every subsequent dispatch must still use the Instance snapshot.
				if err := db.WithContext(ctx).Model(&model.JobDefinition{}).Where("id = ?", request.JobId).
					Updates(map[string]any{"processor_name": "changed.processor", "params": "changed", "max_retries": 0, "timeout_seconds": 1}).Error; err != nil {
					return nil, status.Error(codes.Internal, "test configuration update failed")
				}
				switch request.Params {
				case "busy-once":
					return &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_BUSY}, nil
				case "reject-once":
					return nil, status.Error(codes.PermissionDenied, "test pre-admission rejection")
				}
			}
		}
		return handler(ctx, req)
	})
	go executorServer.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := executorServer.Shutdown(ctx); err != nil {
			t.Errorf("stop completion executor server: %v", err)
		}
	})
	completionHostDiscovery(t, client, executorConfig)
	executorReady, executorReadyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer executorReadyCancel()
	target, err := host.ExecutorClients.Executor(executorReady, group.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Client.CancelInstance(executorReady, &protocol.CancelInstanceRequest{
		Auth: &protocol.GroupAuth{GroupName: group.Name, Token: group.Token}, InstanceId: 1, Reason: protocol.CancelReason_MANUAL_STOP,
	}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("wait for discovered executor: %v", err)
	}

	var wantDispatches, wantBusiness int32
	for _, scenario := range completionHostScenarios(retryScenarios) {
		wantDispatches += int32(scenario.attempts)
		wantBusiness += int32(scenario.businessCalls)
		t.Run(scenario.name, func(t *testing.T) {
			definition := model.JobDefinition{Name: scenario.name, ExecutorGroupID: group.ID, ProcessorName: "test.completion",
				ExecutionMode: model.ExecutionModeNormal, Params: scenario.params, CronExpr: "0 * * * * *",
				Status: basemodel.RecordStatusDisabled, BlockPolicy: model.BlockParallel, MaxRetries: scenario.maxRetries, RetryIntervalSeconds: 1, TimeoutSeconds: 300}
			if err := db.Create(&definition).Error; err != nil {
				t.Fatal(err)
			}
			rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer rpcCancel()
			override := false
			request := &job.TriggerJobRequest{Id: definition.ID, RequestId: uuid.NewString(), ParamsOverridden: &override, OperatorId: 42}
			created, err := logic.NewTriggerJobLogic(rpcCtx, host).TriggerJob(request)
			rpcCancel()
			if err != nil {
				t.Fatalf("trigger production execution: %v", err)
			}
			var parent model.JobInstance
			dispatchHostEventuallyWithin(t, "automatic parent completion from persisted result", 15*time.Second, func(ctx context.Context) bool {
				if err := db.WithContext(ctx).First(&parent, created.InstanceId).Error; err != nil {
					t.Fatal(err)
				}
				return parent.Status == scenario.parentStatus
			})
			var tasks []model.JobTask
			if err := db.Where("instance_id = ?", parent.ID).Find(&tasks).Error; err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 1 {
				t.Fatalf("ordinary execution created %d Tasks", len(tasks))
			}
			task := tasks[0]
			if task.Status != scenario.taskStatus || task.TaskType != model.TaskTypeOrdinary || task.AttemptNo != scenario.attempts || task.UsedAttempts != scenario.usedAttempts ||
				task.FinishedAt == nil || parent.StartedAt == nil || parent.DeadlineAt == nil || parent.FinishedAt == nil ||
				task.ResultSummary == nil || *task.ResultSummary != scenario.summary || parent.ResultSummary == nil || *parent.ResultSummary != scenario.summary ||
				task.EndReason == nil || *task.EndReason != scenario.reason || parent.EndReason == nil || *parent.EndReason != scenario.reason {
				t.Fatal("terminal persistence lost attempt, completion or exact optional summary facts")
			}
			if scenario.taskStatus == model.TaskSucceeded {
				if string(task.ResultData) != "9007199254740993" {
					t.Fatal("successful result lost exact JSON data")
				}
			} else if task.ResultData != nil {
				t.Fatal("failed business output was persisted as successful data")
			}
			queryCtx, queryCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer queryCancel()
			detail, err := logic.NewGetInstanceLogic(queryCtx, host).GetInstance(&job.GetInstanceRequest{Id: parent.ID})
			if err != nil {
				t.Fatal(err)
			}
			if detail.Instance == nil || detail.Instance.Status != int32(scenario.parentStatus) || detail.Instance.ResultSummary == nil ||
				*detail.Instance.ResultSummary != scenario.summary || detail.Instance.FinishedAt == nil ||
				detail.Instance.ExecutionConfig == nil || detail.Instance.ExecutionConfig.Params != scenario.params {
				t.Fatal("management detail did not preserve the frozen input and terminal result")
			}
			listed, err := logic.NewListInstanceTasksLogic(queryCtx, host).ListInstanceTasks(&job.ListInstanceTasksRequest{InstanceId: parent.ID, Page: 1, PageSize: 10})
			if err != nil {
				t.Fatal(err)
			}
			if listed.Total != 1 || len(listed.Items) != 1 || listed.Items[0].Id != task.ID || listed.Items[0].Status != int32(scenario.taskStatus) ||
				listed.Items[0].ResultSummary == nil || *listed.Items[0].ResultSummary != scenario.summary {
				t.Fatal("management Task list did not reflect persisted terminal facts")
			}
			replayed, err := logic.NewTriggerJobLogic(queryCtx, host).TriggerJob(request)
			if err != nil || replayed.InstanceId != parent.ID {
				t.Fatalf("manual request replay did not preserve Instance identity: %v", err)
			}
			calls := processor.snapshot(parent.ID)
			if len(calls) != scenario.businessCalls {
				t.Fatalf("business calls = %d, want %d", len(calls), scenario.businessCalls)
			}
			for _, call := range calls {
				if call.JobID != definition.ID || call.TaskID != task.ID || call.Params != scenario.params {
					t.Fatal("production flow changed the frozen business input or Task identity")
				}
			}
			requests := dispatched.snapshot(parent.ID)
			if int64(len(requests)) != scenario.attempts || task.StartedAt == nil || !task.StartedAt.Equal(*parent.StartedAt) ||
				parent.DeadlineAt.Sub(*parent.StartedAt) != 300*time.Second {
				t.Fatal("retry recreated the Task, reset the original deadline, or sent an attempt more than once")
			}
			for index, request := range requests {
				if request.taskID != task.ID || request.attemptNo != int64(index+1) || !request.deadline.Equal(*parent.DeadlineAt) {
					t.Fatal("dispatch lost the original Task/deadline or reused an attempt number")
				}
			}
		})
	}
	assertDispatchHostFollower(t, leader)
	shutdown, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	for _, drain := range []func(context.Context) error{executorServer.Shutdown, runtime.Drain, resultServer.Shutdown, host.Drain, runtime.Wait, host.Wait} {
		if err := drain(shutdown); err != nil {
			t.Fatalf("drain complete execution under the shared shutdown budget: %v", err)
		}
	}
	if dispatches.Load() != wantDispatches || reports.Load() < wantBusiness || processor.total() != int(wantBusiness) {
		t.Fatalf("unexpected transport or business counts: dispatches=%d reports=%d business=%d", dispatches.Load(), reports.Load(), processor.total())
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
}

func completionHostServerConfig(t *testing.T, endpoint, key string) zrpc.RpcServerConf {
	t.Helper()
	var c zrpc.RpcServerConf
	if err := conf.FillDefault(&c); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.Name, c.Mode = "completion-host-test", service.TestMode
	c.DevServer.Enabled, c.CpuThreshold = false, 0
	c.ListenOn = listener.Addr().String()
	c.Etcd = discov.EtcdConf{Hosts: []string{endpoint}, Key: key}
	_ = listener.Close()
	return c
}

type completionHostScenario struct {
	name, params, summary string
	parentStatus          model.InstanceStatus
	taskStatus            model.TaskStatus
	reason                string
	maxRetries            int32
	attempts              int64
	usedAttempts          int32
	businessCalls         int
}

func completionHostScenarios(retries bool) []completionHostScenario {
	if retries {
		return []completionHostScenario{
			{"failure then success", "retry-success", "business completed", model.InstanceSucceeded, model.TaskSucceeded, model.EndReasonSuccess, 1, 2, 2, 2},
			{"failure exhausts budget", "failure", "business refused work", model.InstanceFailed, model.TaskFailed, model.EndReasonExecutionFailed, 1, 2, 2, 2},
			{"BUSY refunds only the execution opportunity", "busy-once", "business completed", model.InstanceSucceeded, model.TaskSucceeded, model.EndReasonSuccess, 0, 2, 1, 1},
			{"definite rejection consumes an opportunity", "reject-once", "business completed", model.InstanceSucceeded, model.TaskSucceeded, model.EndReasonSuccess, 1, 2, 2, 1},
		}
	}
	return []completionHostScenario{
		{"success summary", "success", "business completed", model.InstanceSucceeded, model.TaskSucceeded, model.EndReasonSuccess, 0, 1, 1, 1},
		{"success empty summary", "success-empty", "", model.InstanceSucceeded, model.TaskSucceeded, model.EndReasonSuccess, 0, 1, 1, 1},
		{"failure summary", "failure", "business refused work", model.InstanceFailed, model.TaskFailed, model.EndReasonExecutionFailed, 0, 1, 1, 1},
		{"failure empty summary", "failure-empty", "", model.InstanceFailed, model.TaskFailed, model.EndReasonExecutionFailed, 0, 1, 1, 1},
	}
}

// completionHostDispatches retains only routing facts, never request credentials.
type completionHostDispatches struct {
	mu       sync.Mutex
	requests map[int64][]completionHostAttempt
}

type completionHostAttempt struct {
	taskID, attemptNo int64
	deadline          time.Time
}

func (d *completionHostDispatches) record(request *protocol.DispatchTaskRequest) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.requests == nil {
		d.requests = make(map[int64][]completionHostAttempt)
	}
	d.requests[request.InstanceId] = append(d.requests[request.InstanceId], completionHostAttempt{
		taskID: request.TaskId, attemptNo: request.AttemptNo, deadline: request.DeadlineAt.AsTime(),
	})
}

func (d *completionHostDispatches) snapshot(id int64) []completionHostAttempt {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]completionHostAttempt(nil), d.requests[id]...)
}

func completionHostDiscovery(t *testing.T, client *clientv3.Client, c zrpc.RpcServerConf) {
	t.Helper()
	dispatchHostEventually(t, "leased execution discovery registration", func(ctx context.Context) bool {
		response, err := client.Get(ctx, c.Etcd.Key+"/", clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		return len(response.Kvs) == 1 && response.Kvs[0].Lease != 0 && string(response.Kvs[0].Value) == c.ListenOn
	})
}

type completionHostProcessor struct {
	mu    sync.Mutex
	calls map[int64][]jobexecutor.Input
}

func (p *completionHostProcessor) Execute(_ context.Context, input jobexecutor.Input) (jobexecutor.Result, error) {
	p.mu.Lock()
	p.calls[input.InstanceID] = append(p.calls[input.InstanceID], input)
	call := len(p.calls[input.InstanceID])
	p.mu.Unlock()
	result := jobexecutor.Result{Data: []byte("9007199254740993")}
	switch input.Params {
	case "retry-success":
		if call == 1 {
			return result, errors.New("first execution failed")
		}
		result.Summary = "business completed"
		return result, nil
	case "success", "busy-once", "reject-once":
		result.Summary = "business completed"
		return result, nil
	case "success-empty":
		return result, nil
	case "failure":
		return result, errors.New("business refused work")
	case "failure-empty":
		return result, errors.New("")
	default:
		return jobexecutor.Result{}, errors.New("unknown completion test scenario")
	}
}

func (p *completionHostProcessor) snapshot(id int64) []jobexecutor.Input {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]jobexecutor.Input(nil), p.calls[id]...)
}

func (p *completionHostProcessor) total() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, calls := range p.calls {
		total += len(calls)
	}
	return total
}
