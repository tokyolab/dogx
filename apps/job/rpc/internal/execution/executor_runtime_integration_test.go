//go:build integration

package execution

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/groupcache"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

// runtimeIntegrationProcessor waits until the dispatch RPC has finished before
// producing a result. The test observes real executor-to-scheduler transport and
// persistence. A separate case releases it before returning the dispatch ACK.
type runtimeIntegrationProcessor struct {
	calls   atomic.Int32
	entered chan context.Context
	release chan struct{}
}

func (p *runtimeIntegrationProcessor) Execute(ctx context.Context, _ jobexecutor.Input) (jobexecutor.Result, error) {
	p.calls.Add(1)
	p.entered <- ctx
	select {
	case <-p.release:
		return jobexecutor.Result{Data: json.RawMessage(`9007199254740993`), Summary: "runtime completed"}, nil
	case <-ctx.Done():
		return jobexecutor.Result{}, ctx.Err()
	}
}

func TestExecutorRuntimeReportsToRealPostgres(t *testing.T) {
	if queueReceiverSubprocess(t, "DOGX_EXECUTOR_RUNTIME_PG_TEST", "TestExecutorRuntimeReportsToRealPostgres") {
		return
	}
	testOrdinaryDispatchRuntime(t, false)
}

func TestOrdinaryDispatchResultPrecedesACK(t *testing.T) {
	if queueReceiverSubprocess(t, "DOGX_DISPATCH_RESULT_FIRST_TEST", "TestOrdinaryDispatchResultPrecedesACK") {
		return
	}
	testOrdinaryDispatchRuntime(t, true)
}

func testOrdinaryDispatchRuntime(t *testing.T, resultBeforeACK bool) {
	t.Helper()
	t.Cleanup(func() { _ = logx.Close() })
	db, _ := testutil.OpenMigratedPostgres(t)
	group, instance := dispatchRuntimeFixture(t, db)
	groups, err := repository.NewExecutorGroupRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := groupcache.New(context.Background(), groups, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewCachedGroupAuthenticator(cache)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	hints := make(chan int64, 4)
	saver, err := taskresult.NewSaver(tasks, taskresult.WithInstanceHint(func(id int64) {
		select {
		case hints <- id:
		default:
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := taskresult.NewQueue(saver, taskresult.QueueConfig{Capacity: 4, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopReceiverQueue(t, queue) })
	if err := queue.Start(); err != nil {
		t.Fatal(err)
	}
	var reports atomic.Int32
	client := queueReceiverClient(t, testReceiver(t, auth, queue), func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == protocol.ResultReceiver_ReportTaskResult_FullMethodName && reports.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "synthetic temporary result outage")
		}
		return handler(ctx, req)
	})
	processor := &runtimeIntegrationProcessor{entered: make(chan context.Context, 1), release: make(chan struct{})}
	registry := jobexecutor.NewRegistry()
	if err := registry.Register(instance.ExecutionConfig.ProcessorName, processor); err != nil {
		t.Fatal(err)
	}
	runtime, err := jobexecutor.NewRuntime(registry, client, group.Name, group.Token, receiverTestID,
		jobexecutor.RuntimeConfig{Concurrency: 1, QueueCapacity: 1, ReportCapacity: 2, ReportConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-processor.release:
		default:
			close(processor.release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Drain(ctx); err != nil {
			t.Errorf("drain executor runtime: %v", err)
		}
	})
	if err := runtime.Start(); err != nil {
		t.Fatal(err)
	}
	executorAuth, err := jobexecutor.NewAuthenticator(group.Name, group.Token)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := jobexecutor.NewRPCAdapter(executorAuth, registry, receiverTestID, runtime)
	if err != nil {
		t.Fatal(err)
	}
	config := transportServerConfig(t)
	server, err := jobexecutor.NewServer(config, func() *jobexecutor.RPCAdapter { return adapter })
	if err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	server.AddUnaryInterceptors(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod != protocol.Executor_DispatchTask_FullMethodName {
			return handler(ctx, req)
		}
		sends.Add(1)
		// The real receiver may report faster than the scheduler saves its ACK.
		// Delay only this test response until the real result queue confirms commit.
		response, err := handler(ctx, req)
		if err != nil || !resultBeforeACK {
			return response, err
		}
		close(processor.release)
		select {
		case id := <-hints:
			if id != instance.ID {
				return nil, status.Error(codes.Internal, "unexpected test result parent")
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return response, nil
	})
	go server.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("stop executor server: %v", err)
		}
	})
	conn := transportClient(t, config.ListenOn, false)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatal(err)
	}
	run, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	registrar, err := dispatch.NewRegistrar(run)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(&runtimeIntegrationExecutorProvider{target: ExecutorTarget{
		Group:  repository.ExecutorGroupConfig{ID: group.ID, Name: group.Name, DiscoveryKey: group.DiscoveryKey, Token: group.Token},
		Client: protocol.NewExecutorClient(conn),
	}}, DefaultDispatchRPCTimeout)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := dispatch.NewDispatcher(registrar, sender, tasks)
	if err != nil {
		t.Fatal(err)
	}
	dispatchCtx, dispatchCancel := context.WithCancel(ctx)
	response, err := dispatcher.DispatchOrdinary(dispatchCtx, instance.ID)
	dispatchCancel()
	if err != nil || !response.Registered || response.Outcome != dispatch.OutcomeAccepted || response.Applied == resultBeforeACK {
		t.Fatalf("dispatch did not acknowledge actual admission: %v", err)
	}
	var task model.JobTask
	if err := db.Where("instance_id = ?", instance.ID).Take(&task).Error; err != nil {
		t.Fatal(err)
	}
	if resultBeforeACK {
		if task.Status != model.TaskSucceeded || task.ExecutorInstanceID != nil || task.ExecutorAddress != nil {
			t.Fatal("late ACK overwrote a result that committed first")
		}
	} else if task.Status != model.TaskRunning || task.ExecutorInstanceID == nil || task.ExecutorInstanceID.String() != receiverTestID ||
		task.ExecutorAddress == nil || *task.ExecutorAddress != config.ListenOn {
		t.Fatal("ACK did not bind the executor process and actual server peer")
	}
	select {
	case businessCtx := <-processor.entered:
		if !resultBeforeACK && businessCtx.Err() != nil {
			t.Fatal("business inherited the completed dispatch lifetime")
		}
	case <-ctx.Done():
		t.Fatal("accepted work did not start")
	}
	if !resultBeforeACK {
		close(processor.release)
		waitReceiverQueueHint(t, ctx, hints, instance.ID)
	}
	if err := runtime.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := queue.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	saved := receiverQueueReadTask(t, db, task.ID)
	if saved.Status != model.TaskSucceeded || string(saved.ResultData) != "9007199254740993" ||
		saved.AttemptNo != task.AttemptNo || saved.UsedAttempts != task.UsedAttempts ||
		saved.ResultSummary == nil || *saved.ResultSummary != "runtime completed" {
		t.Fatal("executor result was not persisted with original attempt and exact JSON")
	}
	replayed, err := dispatcher.DispatchOrdinary(ctx, instance.ID)
	if err != nil || replayed.Registered || sends.Load() != 1 || processor.calls.Load() != 1 || reports.Load() != 2 {
		t.Fatalf("dispatch/report retry reran business or lost retry: sends=%d business=%d reports=%d err=%v", sends.Load(), processor.calls.Load(), reports.Load(), err)
	}
	var parent model.JobInstance
	if err := db.Where("id = ?", instance.ID).Take(&parent).Error; err != nil {
		t.Fatal(err)
	}
	if parent.Status != model.InstanceRunning || parent.FinishedAt != nil {
		t.Fatal("executor runtime test unexpectedly advanced parent completion")
	}
}

type runtimeIntegrationExecutorProvider struct{ target ExecutorTarget }

func (p *runtimeIntegrationExecutorProvider) Executor(context.Context, int64) (ExecutorTarget, error) {
	return p.target, nil
}

func dispatchRuntimeFixture(t *testing.T, db *gorm.DB) (model.ExecutorGroup, model.JobInstance) {
	t.Helper()
	group := model.ExecutorGroup{Name: "runtime-workers", DiscoveryKey: "test.runtime.rpc", Token: receiverTestToken}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	job := model.JobDefinition{Name: "ordinary dispatch test", ExecutorGroupID: group.ID,
		ProcessorName: "test.runtime", ExecutionMode: model.ExecutionModeNormal,
		CronExpr: "0 * * * * *", MaxRetries: 3, RetryIntervalSeconds: 10, TimeoutSeconds: 900}
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	now, requestID, operatorID := time.Now().UTC(), uuid.New(), int64(1)
	instance := model.JobInstance{JobID: job.ID, JobName: job.Name, ExecutorGroupID: group.ID,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: job.ProcessorName,
			ExecutionMode: model.ExecutionModeNormal, MaxRetries: 3, RetryIntervalSeconds: 10, TimeoutSeconds: 900},
		Status: model.InstanceWaiting, CreatedAt: now, UpdatedAt: now}
	if err := db.Create(&instance).Error; err != nil {
		t.Fatal(err)
	}
	return group, instance
}
