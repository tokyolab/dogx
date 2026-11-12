//go:build integration

package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/groupcache"
	"github.com/tokyolab/dogx/apps/job/internal/instancecheck"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// shardedRuntimeProcessor records each received single JSON value and echoes it
// as output, so the test observes both directions without float64 conversion.
type shardedRuntimeProcessor struct {
	preCalls atomic.Int32
	mu       sync.Mutex
	items    map[int64][]json.RawMessage
}

func (p *shardedRuntimeProcessor) PreProcess(context.Context, jobexecutor.Input) (jobexecutor.PrepareResult, error) {
	p.preCalls.Add(1)
	return jobexecutor.PrepareResult{Items: []json.RawMessage{
		json.RawMessage(`null`), json.RawMessage(`9007199254740993`), json.RawMessage(`[1,9007199254740993,null]`),
	}, Summary: "preparation completed"}, nil
}

func (p *shardedRuntimeProcessor) ShardingProcess(_ context.Context, input jobexecutor.ShardInput) (jobexecutor.Result, error) {
	p.mu.Lock()
	p.items[input.TaskID] = append(p.items[input.TaskID], append(json.RawMessage(nil), input.Item...))
	p.mu.Unlock()
	return jobexecutor.Result{Data: input.Item, Summary: "shard completed"}, nil
}

func TestShardedRuntimeGeneratesAndDispatchesSavedItems(t *testing.T) {
	if queueReceiverSubprocess(t, "DOGX_SHARDED_RUNTIME_PG_TEST", "TestShardedRuntimeGeneratesAndDispatchesSavedItems") {
		return
	}
	t.Cleanup(func() { _ = logx.Close() })
	db, _ := testutil.OpenMigratedPostgres(t)
	group, instance := dispatchRuntimeFixture(t, db)
	instance.ExecutionConfig.ExecutionMode = model.ExecutionModeSharded
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("execution_config", instance.ExecutionConfig).Error; err != nil {
		t.Fatal(err)
	}
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
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	hints := make(chan int64, 8)
	saver, err := taskresult.NewSaver(tasks, taskresult.WithInstanceHint(func(id int64) {
		select {
		case hints <- id:
		default:
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := taskresult.NewQueue(saver, taskresult.QueueConfig{Capacity: 8, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopReceiverQueue(t, queue) })
	if err := queue.Start(); err != nil {
		t.Fatal(err)
	}
	client := queueReceiverClient(t, testReceiver(t, auth, queue))
	processor := &shardedRuntimeProcessor{items: make(map[int64][]json.RawMessage)}
	registry := jobexecutor.NewRegistry()
	if err := registry.RegisterSharding(instance.ExecutionConfig.ProcessorName, processor); err != nil {
		t.Fatal(err)
	}
	runtime, err := jobexecutor.NewRuntime(registry, client, group.Name, group.Token, receiverTestID,
		jobexecutor.RuntimeConfig{Concurrency: 3, QueueCapacity: 3, ReportCapacity: 4, ReportConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Drain(ctx); err != nil {
			t.Errorf("drain sharded executor runtime: %v", err)
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
	serverConfig := transportServerConfig(t)
	server, err := jobexecutor.NewServer(serverConfig, func() *jobexecutor.RPCAdapter { return adapter })
	if err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	server.AddUnaryInterceptors(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == protocol.Executor_DispatchTask_FullMethodName {
			sends.Add(1)
		}
		return handler(ctx, req)
	})
	go server.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("stop sharded executor server: %v", err)
		}
	})
	conn := transportClient(t, serverConfig.ListenOn, false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
	response, err := dispatcher.DispatchInstance(ctx, instance.ID)
	if err != nil || !response.Registered || response.Outcome != dispatch.OutcomeAccepted {
		t.Fatalf("preparation dispatch: %+v, %v", response, err)
	}
	waitReceiverQueueHint(t, ctx, hints, instance.ID)
	pre, err := tasks.FindPreparation(ctx, instance.ID)
	if err != nil || pre == nil || pre.Status != model.TaskSucceeded {
		t.Fatalf("preparation was not durably saved: %+v, %v", pre, err)
	}
	if exists, err := tasks.HasShards(ctx, instance.ID); err != nil || exists {
		t.Fatalf("shards appeared before generation: %v, %v", exists, err)
	}
	prepared, err := tasks.FindPreparationResult(ctx, instance.ID)
	if err != nil || prepared == nil {
		t.Fatalf("saved preparation output: %+v, %v", prepared, err)
	}
	var generationHints atomic.Int32
	generator, err := instancecheck.NewGenerator(run, func(id int64) {
		if id == instance.ID {
			generationHints.Add(1)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if generated, err := generator.Generate(ctx, instance.ID, *prepared); err != nil || !generated {
		t.Fatalf("generate saved items: %v, %v", generated, err)
	}
	if generated, err := generator.Generate(ctx, instance.ID, *prepared); err != nil || generated || generationHints.Load() != 1 {
		t.Fatalf("duplicate generation created work: %v, %v, hints=%d", generated, err, generationHints.Load())
	}
	scanNow, err := instances.DatabaseTime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := tasks.ListInstanceDispatchCandidates(ctx, instance.ID, scanNow, 128)
	if err != nil || len(candidates) != 3 {
		t.Fatalf("due shards: %+v, %v", candidates, err)
	}
	for _, candidate := range candidates {
		if candidate.TaskType != model.TaskTypeShard || candidate.Status != model.TaskWaiting || candidate.AttemptNo != 0 {
			t.Fatalf("unexpected first candidate: %+v", candidate)
		}
		response, err := dispatcher.DispatchTask(ctx, candidate)
		if err != nil || !response.Registered || response.Outcome != dispatch.OutcomeAccepted {
			t.Fatalf("shard dispatch: %+v, %v", response, err)
		}
	}
	for range candidates {
		waitReceiverQueueHint(t, ctx, hints, instance.ID)
	}
	for _, candidate := range candidates {
		response, err := dispatcher.DispatchTask(ctx, candidate)
		if err != nil || response.Registered {
			t.Fatalf("replayed first candidate dispatched again: %+v, %v", response, err)
		}
	}
	if response, err := dispatcher.DispatchInstance(ctx, instance.ID); err != nil || response.Registered {
		t.Fatalf("preparation start replayed: %+v, %v", response, err)
	}
	if err := runtime.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := queue.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	var saved []model.JobTask
	if err := db.Where("instance_id = ? AND task_type = ?", instance.ID, model.TaskTypeShard).Order("id").Find(&saved).Error; err != nil {
		t.Fatal(err)
	}
	want := []string{"null", "9007199254740993", "[1,9007199254740993,null]"}
	if len(saved) != len(want) || sends.Load() != 4 || processor.preCalls.Load() != 1 {
		t.Fatalf("unexpected execution counts: saved=%d sends=%d preparations=%d", len(saved), sends.Load(), processor.preCalls.Load())
	}
	processor.mu.Lock()
	defer processor.mu.Unlock()
	for index, task := range saved {
		calls := processor.items[task.ID]
		if task.Status != model.TaskSucceeded || task.AttemptNo != 1 || task.UsedAttempts != 1 || len(calls) != 1 {
			t.Fatalf("shard was not executed exactly once: task=%d state=%d calls=%d", task.ID, task.Status, len(calls))
		}
		for _, data := range [][]byte{task.InputData, task.ResultData, calls[0]} {
			var compact bytes.Buffer
			if err := json.Compact(&compact, data); err != nil || compact.String() != want[index] {
				t.Fatalf("single shard input changed type, shape or precision: task=%d error=%v", task.ID, err)
			}
		}
	}
	var parent model.JobInstance
	if err := db.Where("id = ?", instance.ID).Take(&parent).Error; err != nil {
		t.Fatal(err)
	}
	if parent.Status != model.InstanceRunning || parent.FinishedAt != nil {
		t.Fatal("component integration advanced parent completion")
	}
}
