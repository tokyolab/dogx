package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

func setShardedSnapshot(t *testing.T, f *registrationFixture, post bool) {
	t.Helper()
	var config model.ExecutionConfig
	if err := json.Unmarshal(f.instance.ExecutionConfig, &config); err != nil {
		t.Fatal(err)
	}
	config.ExecutionMode, config.PostEnabled = model.ExecutionModeSharded, post
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	f.instance.ExecutionConfig = data
}

func TestRegistrarStartsPreparationOnlyAfterConfirmedCommit(t *testing.T) {
	for _, commitFailure := range []bool{false, true} {
		f := newRegistrationFixture(t)
		setShardedSnapshot(t, f, true)
		if commitFailure {
			f.commitErr = errors.New("synthetic unknown commit")
		}
		got, err := f.registrar.StartInstance(context.Background(), f.instance.ID)
		if !f.started || !f.inserted || f.insertedType != model.TaskTypePrepare {
			t.Fatal("sharded start did not create preparation with its parent transition")
		}
		if commitFailure {
			if got != nil || !errors.Is(err, f.commitErr) {
				t.Fatalf("unconfirmed preparation escaped: %+v, %v", got, err)
			}
		} else if err != nil || got == nil || got.TaskType != model.TaskTypePrepare || got.AttemptNo != 1 || got.InputData != nil || !got.ExecutionConfig.PostEnabled {
			t.Fatalf("preparation attempt: %+v, %v", got, err)
		}
	}
}

func TestRegistrarRejectsMissingOrNullFrozenMembers(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		for _, member := range []string{"post_enabled", "max_retries", "params"} {
			for _, missing := range []bool{false, true} {
				f := newRegistrationFixture(t)
				if sharded {
					setShardedSnapshot(t, f, false)
				}
				var members map[string]json.RawMessage
				if err := json.Unmarshal(f.instance.ExecutionConfig, &members); err != nil {
					t.Fatal(err)
				}
				if missing {
					delete(members, member)
				} else {
					members[member] = json.RawMessage(`null`)
				}
				data, err := json.Marshal(members)
				if err != nil {
					t.Fatal(err)
				}
				f.instance.ExecutionConfig = data
				if got, err := f.registrar.StartInstance(context.Background(), f.instance.ID); got != nil || !errors.Is(err, ErrInvalidExecutionConfig) || f.clockRead {
					t.Fatalf("invalid frozen member %s accepted: %+v, %v", member, got, err)
				}
			}
		}
	}
}

func TestRegistrarRegistersFirstShardsAndPostAndAllStageRetries(t *testing.T) {
	for _, taskType := range []model.TaskType{model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
		for _, first := range []bool{false, true} {
			if first && (taskType == model.TaskTypeOrdinary || taskType == model.TaskTypePrepare) {
				continue
			}
			t.Run(fmt.Sprintf("type_%d_first_%v", taskType, first), func(t *testing.T) {
				f, candidate := newAttemptRegistrationFixture(t)
				if taskType != model.TaskTypeOrdinary {
					setShardedSnapshot(t, f, true)
				}
				candidate.TaskType = taskType
				if first {
					candidate.Status, candidate.AttemptNo = model.TaskWaiting, 0
				}
				f.retryTask.TaskType, f.retryTask.AttemptNo = taskType, candidate.AttemptNo+1
				switch taskType {
				case model.TaskTypeShard:
					f.retryTask.InputData = model.TaskData(`{"id":9007199254740993}`)
				case model.TaskTypePost:
					f.retryTask.InputData = model.TaskData(`{"total":2,"succeeded":1,"failed":1}`)
				}
				got, err := f.registrar.RegisterTask(context.Background(), candidate)
				if err != nil || got == nil || got.TaskType != taskType || got.AttemptNo != candidate.AttemptNo+1 || !reflect.DeepEqual(got.InputData, f.retryTask.InputData) {
					t.Fatalf("registered stage: %+v, %v", got, err)
				}
				if f.retryWrite.TaskType != candidate.TaskType || f.retryWrite.ExpectedStatus != candidate.Status || f.retryWrite.ExpectedAttemptNo != candidate.AttemptNo {
					t.Fatalf("candidate identity changed: %+v", f.retryWrite)
				}
				if len(got.InputData) != 0 {
					original := string(got.InputData)
					f.retryTask.InputData[0] = ' '
					if string(got.InputData) != original {
						t.Fatal("registered input aliases repository-owned bytes")
					}
				}
			})
		}
	}
}

func TestRegistrarRejectsStageConfigurationMismatchAndInvalidFirstCandidates(t *testing.T) {
	for _, taskType := range []model.TaskType{model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
		f, candidate := newAttemptRegistrationFixture(t)
		candidate.TaskType = taskType
		if got, err := f.registrar.RegisterTask(context.Background(), candidate); got != nil || !errors.Is(err, ErrInvalidInstanceState) || f.retryCalls != 0 {
			t.Fatalf("ordinary snapshot accepted stage %d: %+v, %v", taskType, got, err)
		}
	}
	f, candidate := newAttemptRegistrationFixture(t)
	setShardedSnapshot(t, f, false)
	candidate.TaskType = model.TaskTypePost
	if got, err := f.registrar.RegisterTask(context.Background(), candidate); got != nil || !errors.Is(err, ErrInvalidInstanceState) || f.retryCalls != 0 {
		t.Fatalf("disabled Post registered: %+v, %v", got, err)
	}
	for _, taskType := range []model.TaskType{model.TaskTypeOrdinary, model.TaskTypePrepare, 0, 5} {
		f, candidate := newAttemptRegistrationFixture(t)
		candidate.TaskType, candidate.Status, candidate.AttemptNo = taskType, model.TaskWaiting, 0
		if got, err := f.registrar.RegisterTask(context.Background(), candidate); got != nil || err == nil || f.called {
			t.Fatalf("invalid first candidate accepted: %+v, %v", candidate, err)
		}
	}
}

func TestDispatcherFirstShardRequiresConfirmedRegistration(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		attempt := dispatcherAttempt()
		attempt.TaskType, attempt.InputData = model.TaskTypeShard, model.TaskData(`null`)
		candidate := repository.DispatchCandidate{TaskID: attempt.TaskID, InstanceID: attempt.InstanceID,
			TaskType: model.TaskTypeShard, Status: model.TaskWaiting}
		registrar := &dispatcherRegistrarStub{retryCall: func(_ context.Context, got repository.DispatchCandidate) (*RegisteredAttempt, error) {
			if got != candidate {
				t.Fatal("first-shard candidate identity changed")
			}
			if !confirmed {
				return &attempt, errors.New("synthetic lost commit confirmation")
			}
			return &attempt, nil
		}}
		sender := &dispatcherSenderStub{call: func(_ context.Context, got RegisteredAttempt) (Response, error) {
			if !reflect.DeepEqual(got, attempt) {
				t.Fatal("dispatcher changed fixed input")
			}
			return Response{}, nil
		}}
		d, err := NewDispatcher(registrar, sender, &dispatcherStoreStub{})
		if err != nil {
			t.Fatal(err)
		}
		result, err := d.DispatchTask(context.Background(), candidate)
		if result.Registered != confirmed || (err == nil) != confirmed || (sender.calls.Load() == 1) != confirmed {
			t.Fatalf("unconfirmed send protection failed: %+v, %v, sends=%d", result, err, sender.calls.Load())
		}
	}
}

func TestRuntimeTaskHintUsesReservedCapacityAndPreservesBothHintMeanings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queryGate := make(chan struct{})
		candidate := repository.DispatchCandidate{TaskID: 31, InstanceID: 11, TaskType: model.TaskTypeShard,
			Status: model.TaskWaiting, NextDispatchAt: time.Now()}
		queries := 0
		tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{},
			listInstance: func(_ context.Context, id int64, at time.Time, limit int) ([]repository.DispatchCandidate, error) {
				queries++
				if id != 11 || limit != 2 || at.Before(candidate.NextDispatchAt) {
					t.Errorf("invalid hinted query: id=%d limit=%d time=%v", id, limit, at)
				}
				<-queryGate
				return []repository.DispatchCandidate{candidate}, nil
			}}
		registrar := &dispatcherRegistrarStub{retryCall: func(_ context.Context, got repository.DispatchCandidate) (*RegisteredAttempt, error) {
			if got != candidate {
				t.Error("hint replaced the original first-attempt identity")
			}
			return nil, nil
		}}
		config := runtimeTestConfig()
		config.WaitingCapacity, config.Concurrency = 2, 1
		r := runtimeForTest(t, &runtimeSourceStub{}, registrar, &dispatcherSenderStub{}, tasks, config)
		startRuntime(t, r)
		synctest.Wait()
		if !r.HintTasks(11) {
			t.Fatal("Task hint rejected")
		}
		synctest.Wait()
		if !r.HintTasks(11) || r.Hint(11) || r.HintTasks(12) {
			t.Fatal("in-flight Task query did not reserve shared waiting capacity or deduplicate")
		}
		close(queryGate)
		synctest.Wait()
		if queries != 1 || registrar.retryCalls.Load() != 1 || registrar.calls.Load() != 0 {
			t.Fatal("Task hint was replayed or confused with first Instance start")
		}
		if !r.Hint(11) {
			t.Fatal("WAITING hint did not reuse released shared capacity")
		}
		synctest.Wait()
		if registrar.calls.Load() != 1 {
			t.Fatal("WAITING hint did not preserve first-start semantics")
		}
		r.Stop()
		if r.HintTasks(11) {
			t.Fatal("stopped runtime admitted a Task hint")
		}
		if err := r.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRuntimeTaskHintFailuresRetainBackoffAcrossFreshHints(t *testing.T) {
	for _, clockFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("database_time_failure_%v", clockFailure), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var clockCalls, hintQueries atomic.Int32
				var failClock atomic.Bool
				source := &runtimeSourceStub{databaseTime: func(context.Context) (time.Time, error) {
					clockCalls.Add(1)
					if failClock.Load() {
						return time.Time{}, errors.New("synthetic database time failure")
					}
					return time.Now(), nil
				}}
				tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{},
					listInstance: func(context.Context, int64, time.Time, int) ([]repository.DispatchCandidate, error) {
						hintQueries.Add(1)
						return nil, errors.New("synthetic hint page failure")
					}}
				config := runtimeTestConfig()
				config.ScanInterval, config.WaitingCapacity = time.Second, 2
				r := runtimeForTest(t, source, &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, tasks, config)
				startRuntime(t, r)
				synctest.Wait()
				if clockCalls.Load() != 1 {
					t.Fatal("initial due scan did not establish its clock")
				}
				failClock.Store(clockFailure)
				if !r.HintTasks(11) {
					t.Fatal("first Task hint rejected")
				}
				synctest.Wait()
				if clockCalls.Load() != 2 {
					t.Fatal("hint did not make its first bounded clock request")
				}
				for range 20 {
					r.HintTasks(11)
					r.HintTasks(12)
					synctest.Wait()
				}
				time.Sleep(799 * time.Millisecond)
				synctest.Wait()
				if clockCalls.Load() != 2 {
					t.Fatal("fresh hints bypassed error backoff before its earliest deadline")
				}
				time.Sleep(402 * time.Millisecond)
				synctest.Wait()
				if got := clockCalls.Load(); got < 3 || got > 4 {
					t.Fatalf("backoff did not resume a bounded hint/global turn: clocks=%d", got)
				}
				if clockFailure && hintQueries.Load() != 0 {
					t.Fatal("hint queried Tasks after its database time failed")
				}
				if !clockFailure && hintQueries.Load() != 2 {
					t.Fatalf("hint page failure lost its backoff: pages=%d", hintQueries.Load())
				}
				stopRuntime(t, r)
			})
		})
	}
}

func TestRuntimeFullHintSupplyCannotStarveEitherDurableSource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &runtimeSourceStub{maximum: func(context.Context) (int64, error) { return 100, nil }}
		var hintQueries atomic.Int32
		tasks := &runtimeTaskStoreStub{responseStore: &dispatcherStoreStub{},
			listInstance: func(context.Context, int64, time.Time, int) ([]repository.DispatchCandidate, error) {
				hintQueries.Add(1)
				return nil, nil
			}}
		config := runtimeTestConfig()
		config.WaitingCapacity = 2
		r := runtimeForTest(t, source, &dispatcherRegistrarStub{}, &dispatcherSenderStub{}, tasks, config)
		// Drive the serial scanner one page at a time so the producer can refill
		// all shared capacity before each selection, without timing assumptions.
		r.started = true
		defer func() {
			r.started = false
			r.Stop()
		}()
		var scans [2]candidateScan
		next, nextHint := waitingScan, int64(1)
		for _, wantKind := range []int{taskHintPage, waitingScan, taskHintPage, taskScan} {
			for r.HintTasks(nextHint) {
				nextHint++
			}
			kind, limit, ok := r.nextPage(&scans, &next)
			if !ok || kind != wantKind || limit != 1 || r.waiting != config.WaitingCapacity {
				t.Fatalf("full hint supply displaced a durable turn: kind=%d want=%d limit=%d waiting=%d", kind, wantKind, limit, r.waiting)
			}
			var hintState candidateScan
			var state *candidateScan
			if kind == taskHintPage {
				hintState.hintID = r.activeHint.instanceID
				state = &hintState
			} else {
				state = &scans[kind]
			}
			page, _, err := r.readPage(kind, state, limit)
			if err != nil || !r.admitPage(page, kind, state.generation) {
				t.Fatalf("bounded page did not return: %v", err)
			}
		}
		if hintQueries.Load() != 2 || len(source.snapshot()) != 1 || len(tasks.snapshot()) != 1 || r.pagesLeft != 0 {
			t.Fatal("hint traffic bypassed source rotation or the shared four-page budget")
		}
	})
}
