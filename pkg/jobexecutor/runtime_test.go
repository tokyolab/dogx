package jobexecutor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"google.golang.org/grpc"
)

// All runtime-specific test doubles live here, including their full interfaces.
type runtimeProcessorFunc func(context.Context, Input) (Result, error)

func (f runtimeProcessorFunc) Execute(ctx context.Context, input Input) (Result, error) {
	return f(ctx, input)
}

type runtimeShardingStub struct {
	pre   func(context.Context, Input) (PrepareResult, error)
	shard func(context.Context, ShardInput) (Result, error)
	post  func(context.Context, PostInput) (Result, error)
}

func (s *runtimeShardingStub) PreProcess(ctx context.Context, input Input) (PrepareResult, error) {
	return s.pre(ctx, input)
}

func (s *runtimeShardingStub) ShardingProcess(ctx context.Context, input ShardInput) (Result, error) {
	return s.shard(ctx, input)
}

func (s *runtimeShardingStub) PostProcess(ctx context.Context, input PostInput) (Result, error) {
	return s.post(ctx, input)
}

type runtimeReportClientStub struct {
	report func(context.Context, *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error)
}

func (s *runtimeReportClientStub) ReportTaskResult(ctx context.Context, request *protocol.ReportTaskResultRequest, _ ...grpc.CallOption) (*protocol.ReportTaskResultResponse, error) {
	if s.report != nil {
		return s.report(ctx, request)
	}
	return &protocol.ReportTaskResultResponse{}, nil
}

func newRuntimeForTest(t *testing.T, registry *Registry, client protocol.ResultReceiverClient, config RuntimeConfig) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(registry, client, adapterTestGroup, adapterTestToken, adapterTestID, config)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func runtimeRegistryForTest(t *testing.T, process runtimeProcessorFunc) *Registry {
	t.Helper()
	registry := NewRegistry()
	if err := registry.Register("ordinary", process); err != nil {
		t.Fatal(err)
	}
	return registry
}

func runtimeInvocation(taskID int64) Invocation {
	return Invocation{
		Input:     Input{JobID: 11, InstanceID: 22, TaskID: taskID, Params: "frozen input"},
		AttemptNo: 1, ProcessorName: "ordinary", Stage: StageOrdinary,
		DeadlineAt: time.Now().Add(time.Hour),
	}
}

func acceptRuntimeTask(t *testing.T, runtime *Runtime, invocation Invocation) {
	t.Helper()
	if accepted, err := runtime.Accept(context.Background(), invocation); err != nil || !accepted {
		t.Fatalf("Accept = (%v, %v), want (true, nil)", accepted, err)
	}
}

func finishRuntime(t *testing.T, runtime *Runtime) {
	t.Helper()
	runtime.Stop()
	if err := runtime.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRejectsInvalidConstruction(t *testing.T) {
	for _, field := range []string{"business concurrency", "waiting capacity", "report capacity", "report concurrency"} {
		t.Run(field, func(t *testing.T) {
			var config RuntimeConfig
			switch field {
			case "business concurrency":
				config.Concurrency = -1
			case "waiting capacity":
				config.QueueCapacity = -1
			case "report capacity":
				config.ReportCapacity = -1
			case "report concurrency":
				config.ReportConcurrency = -1
			}
			if runtime, err := NewRuntime(NewRegistry(), &runtimeReportClientStub{}, adapterTestGroup, adapterTestToken, adapterTestID, config); err == nil || runtime != nil {
				t.Fatalf("invalid config returned runtime=%v, error=%v", runtime != nil, err)
			}
		})
	}
	var typedNil *runtimeReportClientStub
	for _, test := range []struct {
		name     string
		registry *Registry
		client   protocol.ResultReceiverClient
		group    string
		token    string
		id       string
	}{
		{"nil registry", nil, &runtimeReportClientStub{}, adapterTestGroup, adapterTestToken, adapterTestID},
		{"nil client", NewRegistry(), nil, adapterTestGroup, adapterTestToken, adapterTestID},
		{"typed nil client", NewRegistry(), typedNil, adapterTestGroup, adapterTestToken, adapterTestID},
		{"invalid group", NewRegistry(), &runtimeReportClientStub{}, "", adapterTestToken, adapterTestID},
		{"invalid token", NewRegistry(), &runtimeReportClientStub{}, adapterTestGroup, "invalid", adapterTestID},
		{"invalid identity", NewRegistry(), &runtimeReportClientStub{}, adapterTestGroup, adapterTestToken, "invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if runtime, err := NewRuntime(test.registry, test.client, test.group, test.token, test.id, RuntimeConfig{}); err == nil || runtime != nil {
				t.Fatal("invalid dependencies were accepted")
			}
		})
	}
}

func TestRuntimeLifecycleAndRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := NewRegistry()
		runtime := newRuntimeForTest(t, registry, &runtimeReportClientStub{}, RuntimeConfig{})
		if err := runtime.Wait(context.Background()); !errors.Is(err, ErrNotReady) {
			t.Fatalf("Wait before Start = %v", err)
		}
		if accepted, err := runtime.Accept(context.Background(), runtimeInvocation(1)); accepted || !errors.Is(err, ErrNotReady) {
			t.Fatalf("Accept before Start = (%v, %v)", accepted, err)
		}
		process := runtimeProcessorFunc(func(context.Context, Input) (Result, error) { return Result{}, nil })
		if err := registry.Register("ordinary", process); err != nil {
			t.Fatalf("construction froze registry before Start: %v", err)
		}
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		if err := registry.Register("late", process); !errors.Is(err, ErrRegistryFrozen) {
			t.Fatalf("registration after Start = %v", err)
		}
		if err := runtime.Start(); err == nil {
			t.Fatal("runtime started twice")
		}
		acceptRuntimeTask(t, runtime, runtimeInvocation(1))
		finishRuntime(t, runtime)
		if err := runtime.Start(); !errors.Is(err, ErrStopping) {
			t.Fatalf("Start after Stop = %v", err)
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := runtime.Wait(cancelled); err != nil {
			t.Fatalf("completed Wait lost to cancelled caller: %v", err)
		}
	})
}

func TestRuntimeStopBeforeStart(t *testing.T) {
	runtime := newRuntimeForTest(t, NewRegistry(), &runtimeReportClientStub{}, RuntimeConfig{})
	runtime.Stop()
	runtime.Stop()
	if err := runtime.Start(); !errors.Is(err, ErrStopping) {
		t.Fatalf("Start after early Stop = %v", err)
	}
	if accepted, err := runtime.Accept(context.Background(), runtimeInvocation(1)); accepted || !errors.Is(err, ErrStopping) {
		t.Fatalf("Accept after early Stop = (%v, %v)", accepted, err)
	}
	if err := runtime.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeDefaultsBoundBusinessAndWaitingWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan int64, 160)
		release := make(chan struct{})
		var running, peak, completed atomic.Int32
		process := runtimeProcessorFunc(func(_ context.Context, input Input) (Result, error) {
			current := running.Add(1)
			for previous := peak.Load(); current > previous && !peak.CompareAndSwap(previous, current); previous = peak.Load() {
			}
			started <- input.TaskID
			<-release
			running.Add(-1)
			completed.Add(1)
			return Result{}, nil
		})
		runtime := newRuntimeForTest(t, runtimeRegistryForTest(t, process), &runtimeReportClientStub{}, RuntimeConfig{})
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		for id := int64(1); id <= 32; id++ {
			acceptRuntimeTask(t, runtime, runtimeInvocation(id))
			<-started
		}
		for id := int64(33); id <= 160; id++ {
			acceptRuntimeTask(t, runtime, runtimeInvocation(id))
		}
		synctest.Wait()
		if running.Load() != 32 || len(started) != 0 {
			t.Fatalf("running=%d, unexpected starts=%d", running.Load(), len(started))
		}
		if accepted, err := runtime.Accept(context.Background(), runtimeInvocation(161)); accepted || err != nil {
			t.Fatalf("full waiting queue = (%v, %v), want BUSY", accepted, err)
		}
		duplicate := runtimeInvocation(1)
		duplicate.DeadlineAt = time.Now().Add(-time.Second)
		acceptRuntimeTask(t, runtime, duplicate)
		queuedDuplicate := runtimeInvocation(33)
		queuedDuplicate.DeadlineAt = duplicate.DeadlineAt
		acceptRuntimeTask(t, runtime, queuedDuplicate)
		runtime.Stop()
		acceptRuntimeTask(t, runtime, duplicate)
		acceptRuntimeTask(t, runtime, queuedDuplicate)
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if accepted, err := runtime.Accept(cancelled, duplicate); !accepted || err != nil {
			t.Fatalf("active duplicate with cancelled caller = (%v, %v)", accepted, err)
		}
		if accepted, err := runtime.Accept(context.Background(), runtimeInvocation(161)); accepted || !errors.Is(err, ErrStopping) {
			t.Fatalf("new work during Stop = (%v, %v)", accepted, err)
		}
		close(release)
		finishRuntime(t, runtime)
		if completed.Load() != 160 || peak.Load() != 32 {
			t.Fatalf("completed=%d, peak=%d", completed.Load(), peak.Load())
		}
	})
}

func TestRuntimeBusinessContextOutlivesAdmissionButKeepsInstanceDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 1)
		result := make(chan *protocol.ReportTaskResultRequest, 1)
		process := runtimeProcessorFunc(func(ctx context.Context, _ Input) (Result, error) {
			started <- ctx
			<-ctx.Done()
			return Result{}, ctx.Err()
		})
		client := &runtimeReportClientStub{report: func(_ context.Context, request *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error) {
			result <- request
			return &protocol.ReportTaskResultResponse{}, nil
		}}
		runtime := newRuntimeForTest(t, runtimeRegistryForTest(t, process), client, RuntimeConfig{Concurrency: 1})
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		invocation := runtimeInvocation(1)
		invocation.DeadlineAt = time.Now().Add(10 * time.Second)
		requestCtx, cancel := context.WithCancel(context.Background())
		if accepted, err := runtime.Accept(requestCtx, invocation); err != nil || !accepted {
			t.Fatalf("Accept = (%v, %v)", accepted, err)
		}
		businessCtx := <-started
		cancel()
		synctest.Wait()
		if businessCtx.Err() != nil {
			t.Fatal("admission cancellation cancelled accepted business work")
		}
		if deadline, ok := businessCtx.Deadline(); !ok || !deadline.Equal(invocation.DeadlineAt) {
			t.Fatalf("business deadline = %v, present=%v", deadline, ok)
		}
		<-time.After(time.Until(invocation.DeadlineAt))
		report := <-result
		if report.Outcome != protocol.TaskOutcome_FAILURE {
			t.Fatal("business deadline did not produce a failure result")
		}
		finishRuntime(t, runtime)
	})
}

func TestRuntimeCancelSignalsCurrentInstanceWithoutCachingFutureCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan Input, 4)
		contexts := make(chan context.Context, 4)
		entryErrors := make(chan error, 4)
		release := make(chan struct{})
		process := runtimeProcessorFunc(func(ctx context.Context, input Input) (Result, error) {
			entryErrors <- ctx.Err()
			started <- input
			contexts <- ctx
			if input.TaskID == 1 {
				<-release
			}
			return Result{}, ctx.Err()
		})
		runtime := newRuntimeForTest(t, runtimeRegistryForTest(t, process), &runtimeReportClientStub{}, RuntimeConfig{Concurrency: 1, QueueCapacity: 4})
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		first := runtimeInvocation(1)
		acceptRuntimeTask(t, runtime, first)
		<-started
		firstCtx := <-contexts
		if err := <-entryErrors; err != nil {
			t.Fatalf("first business context was cancelled at entry: %v", err)
		}
		acceptRuntimeTask(t, runtime, runtimeInvocation(2))
		other := runtimeInvocation(3)
		other.InstanceID++
		acceptRuntimeTask(t, runtime, other)
		if err := runtime.Cancel(context.Background(), first.InstanceID, CancelManualStop); err != nil {
			t.Fatal(err)
		}
		if firstCtx.Err() == nil {
			t.Fatal("Cancel returned before signalling running business work")
		}
		if err := runtime.Cancel(context.Background(), first.InstanceID, CancelManualStop); err != nil {
			t.Fatalf("repeated Cancel = %v", err)
		}
		// The first processor ignores cancellation until release; Cancel must not wait.
		acceptRuntimeTask(t, runtime, first)
		close(release)
		next := <-started
		<-contexts
		if err := <-entryErrors; next.TaskID != 3 || err != nil {
			t.Fatalf("next processor Task=%d, entry error=%v", next.TaskID, err)
		}
		synctest.Wait()
		if len(started) != 0 {
			t.Fatal("cancelled queued work reached business code")
		}
		late := runtimeInvocation(4)
		acceptRuntimeTask(t, runtime, late)
		if input := <-started; input.TaskID != 4 {
			t.Fatalf("late Task=%d", input.TaskID)
		}
		<-contexts
		if err := <-entryErrors; err != nil {
			t.Fatal("past Instance cancellation was cached for later admission")
		}
		if err := runtime.Cancel(context.Background(), 999, CancelInstanceTimeout); err != nil {
			t.Fatalf("Cancel without local work = %v", err)
		}
		finishRuntime(t, runtime)
	})
}

func TestRuntimeQueuedDeadlineDoesNotStartExpiredBusinessWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan int64, 2)
		release := make(chan struct{})
		process := runtimeProcessorFunc(func(_ context.Context, input Input) (Result, error) {
			started <- input.TaskID
			<-release
			return Result{}, nil
		})
		runtime := newRuntimeForTest(t, runtimeRegistryForTest(t, process), &runtimeReportClientStub{}, RuntimeConfig{Concurrency: 1, QueueCapacity: 1})
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		acceptRuntimeTask(t, runtime, runtimeInvocation(1))
		<-started
		queued := runtimeInvocation(2)
		queued.DeadlineAt = time.Now().Add(time.Second)
		acceptRuntimeTask(t, runtime, queued)
		<-time.After(time.Second)
		close(release)
		finishRuntime(t, runtime)
		if len(started) != 0 {
			t.Fatal("expired queued Task reached business code")
		}
	})
}

func TestRuntimeReleasesActivityAfterBusinessExitBeforeReportAcknowledgement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 2)
		releaseBusiness := make(chan struct{})
		reportStarted := make(chan struct{}, 2)
		releaseReport := make(chan struct{})
		process := runtimeProcessorFunc(func(context.Context, Input) (Result, error) {
			started <- struct{}{}
			<-releaseBusiness
			return Result{}, nil
		})
		client := &runtimeReportClientStub{report: func(ctx context.Context, _ *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error) {
			reportStarted <- struct{}{}
			select {
			case <-releaseReport:
				return &protocol.ReportTaskResultResponse{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		runtime := newRuntimeForTest(t, runtimeRegistryForTest(t, process), client, RuntimeConfig{Concurrency: 1, ReportConcurrency: 1})
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		invocation := runtimeInvocation(1)
		acceptRuntimeTask(t, runtime, invocation)
		<-started
		acceptRuntimeTask(t, runtime, invocation)
		synctest.Wait()
		if len(started) != 0 {
			t.Fatal("active duplicate ran business code twice")
		}
		close(releaseBusiness)
		<-reportStarted
		acceptRuntimeTask(t, runtime, invocation)
		<-started
		close(releaseReport)
		finishRuntime(t, runtime)
	})
}

func TestRuntimeFullReporterKeepsBusinessWorkerBoundUntilHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan int64, 4)
		reports := make(chan *protocol.ReportTaskResultRequest, 4)
		releaseReport := make(chan struct{})
		release := sync.OnceFunc(func() { close(releaseReport) })
		defer release()
		process := runtimeProcessorFunc(func(_ context.Context, input Input) (Result, error) {
			started <- input.TaskID
			return Result{Summary: "business complete"}, nil
		})
		client := &runtimeReportClientStub{report: func(ctx context.Context, request *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error) {
			reports <- request
			if request.TaskId == 1 {
				select {
				case <-releaseReport:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &protocol.ReportTaskResultResponse{}, nil
		}}
		runtime := newRuntimeForTest(t, runtimeRegistryForTest(t, process), client,
			RuntimeConfig{Concurrency: 1, QueueCapacity: 1, ReportCapacity: 1, ReportConcurrency: 1})
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		defer runtime.Stop()
		first := runtimeInvocation(1)
		first.InstanceID = 21
		acceptRuntimeTask(t, runtime, first)
		if taskID := <-started; taskID != first.TaskID {
			t.Fatalf("first business Task = %d", taskID)
		}
		if report := <-reports; report.TaskId != first.TaskID {
			t.Fatalf("first report Task = %d", report.TaskId)
		}
		second := runtimeInvocation(2)
		acceptRuntimeTask(t, runtime, second)
		if taskID := <-started; taskID != second.TaskID {
			t.Fatalf("second business Task = %d", taskID)
		}
		synctest.Wait()

		// The completed activity is gone, but its worker still owns the handoff.
		// Accepting it again must occupy the sole waiting position, not deduplicate.
		acceptRuntimeTask(t, runtime, second)
		third := runtimeInvocation(3)
		third.InstanceID = 23
		if accepted, err := runtime.Accept(context.Background(), third); accepted || err != nil {
			t.Fatalf("full waiting queue during result handoff = (%v, %v), want BUSY", accepted, err)
		}
		synctest.Wait()
		if len(started) != 0 || len(reports) != 0 {
			t.Fatal("blocked result handoff spawned extra business or reporting workers")
		}
		// Cancel skips the new queued duplicate, but cannot withdraw the original
		// completed result which is still waiting for reporting capacity.
		if err := runtime.Cancel(context.Background(), second.InstanceID, CancelManualStop); err != nil {
			t.Fatal(err)
		}
		release()
		finishRuntime(t, runtime)
		if len(started) != 0 || len(reports) != 1 {
			t.Fatalf("unexpected extra business starts=%d, remaining reports=%d", len(started), len(reports))
		}
		report := <-reports
		if report.TaskId != second.TaskID || report.InstanceId != second.InstanceID || report.AttemptNo != second.AttemptNo ||
			report.Outcome != protocol.TaskOutcome_SUCCESS || report.Summary != "business complete" {
			t.Fatal("cancellation withdrew or changed the completed result waiting for handoff")
		}
	})
}

func TestRuntimeDistinctAttemptsAreNotDeduplicated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 2)
		release := make(chan struct{})
		process := runtimeProcessorFunc(func(context.Context, Input) (Result, error) {
			started <- struct{}{}
			<-release
			return Result{}, nil
		})
		runtime := newRuntimeForTest(t, runtimeRegistryForTest(t, process), &runtimeReportClientStub{}, RuntimeConfig{Concurrency: 2})
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		invocation := runtimeInvocation(1)
		acceptRuntimeTask(t, runtime, invocation)
		<-started
		invocation.AttemptNo++
		acceptRuntimeTask(t, runtime, invocation)
		<-started
		close(release)
		finishRuntime(t, runtime)
	})
}

func TestRuntimeRoutesStagesAndPreservesOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const preciseJSON = `{"id":9007199254740993}`
		invocation := runtimeInvocation(1)
		var mu sync.Mutex
		var called []Stage
		checkInput := func(input Input, stage Stage) {
			if input != invocation.Input {
				t.Errorf("stage %d received input %+v", stage, input)
			}
			mu.Lock()
			called = append(called, stage)
			mu.Unlock()
		}
		registry := runtimeRegistryForTest(t, func(_ context.Context, input Input) (Result, error) {
			checkInput(input, StageOrdinary)
			return Result{Data: json.RawMessage(preciseJSON), Summary: "ordinary complete"}, nil
		})
		if err := registry.RegisterSharding("sharding", &runtimeShardingStub{
			pre: func(_ context.Context, input Input) (PrepareResult, error) {
				checkInput(input, StagePrepare)
				return PrepareResult{Items: []json.RawMessage{}, Summary: "preparation complete"}, nil
			},
			shard: func(_ context.Context, input ShardInput) (Result, error) {
				checkInput(input.Input, StageShard)
				if string(input.Item) != preciseJSON {
					t.Errorf("shard item changed: %q", input.Item)
				}
				return Result{Data: json.RawMessage("null"), Summary: "shard complete"}, nil
			},
			post: func(_ context.Context, input PostInput) (Result, error) {
				checkInput(input.Input, StagePost)
				if input.Total != 3 || input.Succeeded != 2 || input.Failed != 1 {
					t.Errorf("post counts changed: %+v", input)
				}
				return Result{Summary: "post complete"}, nil
			},
		}); err != nil {
			t.Fatal(err)
		}
		reports := make(chan *protocol.ReportTaskResultRequest, 4)
		client := &runtimeReportClientStub{report: func(_ context.Context, request *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error) {
			reports <- request
			return &protocol.ReportTaskResultResponse{}, nil
		}}
		runtime := newRuntimeForTest(t, registry, client, RuntimeConfig{Concurrency: 1})
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		for _, stage := range []Stage{StageOrdinary, StagePrepare, StageShard, StagePost} {
			invocation.Stage = stage
			invocation.AttemptNo = int64(stage)
			if stage != StageOrdinary {
				invocation.ProcessorName = "sharding"
				invocation.RequiredPost = true
			}
			if stage == StageShard {
				invocation.Item = json.RawMessage(preciseJSON)
			}
			if stage == StagePost {
				invocation.Total, invocation.Succeeded, invocation.Failed = 3, 2, 1
			}
			acceptRuntimeTask(t, runtime, invocation)
			report := <-reports
			if report.JobId != invocation.JobID || report.InstanceId != invocation.InstanceID || report.TaskId != invocation.TaskID || report.AttemptNo != invocation.AttemptNo || report.ExecutorInstanceId != adapterTestID {
				t.Fatal("report identity differs from admitted invocation")
			}
			if report.Auth.GetGroupName() != adapterTestGroup || report.Auth.GetToken() != adapterTestToken || report.Outcome != protocol.TaskOutcome_SUCCESS {
				t.Fatal("report credentials or business outcome changed")
			}
			switch stage {
			case StageOrdinary:
				if string(report.GetDataJson()) != preciseJSON || report.Summary != "ordinary complete" {
					t.Fatal("ordinary output lost encoded precision or summary")
				}
			case StagePrepare:
				if report.GetPreparedItems() == nil || len(report.GetPreparedItems().Items) != 0 {
					t.Fatal("empty preparation lost its explicit output branch")
				}
			case StageShard:
				if string(report.GetDataJson()) != "null" {
					t.Fatal("explicit JSON null was omitted")
				}
			case StagePost:
				if report.Output != nil {
					t.Fatal("nil Post output became an explicit value")
				}
			}
		}
		finishRuntime(t, runtime)
		if !reflect.DeepEqual(called, []Stage{StageOrdinary, StagePrepare, StageShard, StagePost}) {
			t.Fatalf("processor calls = %v", called)
		}
	})
}

func TestRuntimeOwnsQueuedShardInput(t *testing.T) {
	for _, value := range []string{`null`, `9007199254740993`, `{"id":9007199254740993}`, `[[1,2],{"ids":[3,4]},null]`} {
		t.Run(value, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started, release := make(chan struct{}), make(chan struct{})
				registry := runtimeRegistryForTest(t, func(context.Context, Input) (Result, error) {
					close(started)
					<-release
					return Result{}, nil
				})
				var received string
				if err := registry.RegisterSharding("sharding", &runtimeShardingStub{
					shard: func(_ context.Context, input ShardInput) (Result, error) {
						received = string(input.Item)
						return Result{}, nil
					},
				}); err != nil {
					t.Fatal(err)
				}
				runtime := newRuntimeForTest(t, registry, &runtimeReportClientStub{}, RuntimeConfig{Concurrency: 1})
				if err := runtime.Start(); err != nil {
					t.Fatal(err)
				}
				acceptRuntimeTask(t, runtime, runtimeInvocation(1))
				<-started
				invocation := runtimeInvocation(2)
				invocation.Stage, invocation.ProcessorName = StageShard, "sharding"
				invocation.Item = json.RawMessage(value)
				acceptRuntimeTask(t, runtime, invocation)
				invocation.Item[0] = '!'
				close(release)
				finishRuntime(t, runtime)
				if received != value {
					t.Fatalf("queued shard received %q, want original single value %q", received, value)
				}
			})
		})
	}
}

func TestRuntimeFailuresIgnorePartialOutputAndPanicDoesNotLoseWorker(t *testing.T) {
	for _, stage := range []Stage{StageOrdinary, StagePrepare, StageShard, StagePost} {
		for _, panics := range []bool{false, true} {
			t.Run(fmt.Sprintf("stage_%d/panic_%v", stage, panics), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					const panicDetail = "panic-payload-must-not-cross-the-boundary"
					var calls int
					fail := func() error {
						calls++
						if calls == 1 {
							if panics {
								panic(panicDetail)
							}
							return errors.New("business failed")
						}
						return nil
					}
					registry := runtimeRegistryForTest(t, func(context.Context, Input) (Result, error) {
						err := fail()
						if err != nil {
							return Result{Data: json.RawMessage("invalid partial output")}, err
						}
						return Result{}, nil
					})
					if err := registry.RegisterSharding("sharding", &runtimeShardingStub{
						pre: func(context.Context, Input) (PrepareResult, error) {
							err := fail()
							if err != nil {
								return PrepareResult{Items: []json.RawMessage{json.RawMessage("invalid partial item")}}, err
							}
							return PrepareResult{Items: []json.RawMessage{}}, nil
						},
						shard: func(context.Context, ShardInput) (Result, error) {
							err := fail()
							if err != nil {
								return Result{Data: json.RawMessage("invalid partial output")}, err
							}
							return Result{}, nil
						},
						post: func(context.Context, PostInput) (Result, error) {
							err := fail()
							if err != nil {
								return Result{Data: json.RawMessage("invalid partial output")}, err
							}
							return Result{}, nil
						},
					}); err != nil {
						t.Fatal(err)
					}
					reports := make(chan *protocol.ReportTaskResultRequest, 2)
					client := &runtimeReportClientStub{report: func(_ context.Context, request *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error) {
						reports <- request
						return &protocol.ReportTaskResultResponse{}, nil
					}}
					runtime := newRuntimeForTest(t, registry, client, RuntimeConfig{Concurrency: 1})
					if err := runtime.Start(); err != nil {
						t.Fatal(err)
					}
					invocation := runtimeInvocation(1)
					invocation.Stage = stage
					if stage == StageShard {
						invocation.Item = json.RawMessage(`null`)
					}
					if stage != StageOrdinary {
						invocation.ProcessorName = "sharding"
						invocation.RequiredPost = stage == StagePost
					}
					acceptRuntimeTask(t, runtime, invocation)
					first := <-reports
					if first.Outcome != protocol.TaskOutcome_FAILURE || first.Output != nil {
						t.Fatal("failed processing retained partial successful output")
					}
					if panics && strings.Contains(first.Summary, panicDetail) {
						t.Fatal("panic payload was included in the result summary")
					}
					invocation.TaskID++
					acceptRuntimeTask(t, runtime, invocation)
					if second := <-reports; second.Outcome != protocol.TaskOutcome_SUCCESS {
						t.Fatal("worker did not continue successfully after the first failure")
					}
					finishRuntime(t, runtime)
				})
			})
		}
	}
}

func TestRuntimeDrainUsesFirstDeadlineAndWaitObservesActualExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 1)
		release := make(chan struct{})
		process := runtimeProcessorFunc(func(ctx context.Context, _ Input) (Result, error) {
			started <- ctx
			<-release
			return Result{}, nil
		})
		runtime := newRuntimeForTest(t, runtimeRegistryForTest(t, process), &runtimeReportClientStub{}, RuntimeConfig{Concurrency: 1})
		if err := runtime.Start(); err != nil {
			t.Fatal(err)
		}
		acceptRuntimeTask(t, runtime, runtimeInvocation(1))
		businessCtx := <-started
		firstCtx, firstCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer firstCancel()
		firstDone := make(chan error, 1)
		go func() { firstDone <- runtime.Drain(firstCtx) }()
		synctest.Wait()
		secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
		defer secondCancel()
		secondDone := make(chan error, 1)
		go func() { secondDone <- runtime.Drain(secondCtx) }()
		if err := <-secondDone; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second Drain = %v", err)
		}
		if businessCtx.Err() != nil {
			t.Fatal("later caller shortened the first drain deadline")
		}
		if err := <-firstDone; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first Drain = %v", err)
		}
		synctest.Wait()
		if businessCtx.Err() == nil {
			t.Fatal("first drain deadline did not cancel running business work")
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := runtime.Wait(cancelled); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait falsely acknowledged uncooperative business exit: %v", err)
		}
		close(release)
		if err := runtime.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
