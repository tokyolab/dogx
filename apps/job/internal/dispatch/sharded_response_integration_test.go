//go:build integration

package dispatch

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/instancecheck"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
)

func TestShardedDispatcherPreservesResultsCommittedBeforeEachStageACK(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	ctx := context.Background()
	instance := createRegistrarInstance(t, db)
	instance.ExecutionConfig.ExecutionMode = model.ExecutionModeSharded
	instance.ExecutionConfig.PostEnabled = true
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
		Update("execution_config", instance.ExecutionConfig).Error; err != nil {
		t.Fatal(err)
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	saver, err := taskresult.NewSaver(tasks)
	if err != nil {
		t.Fatal(err)
	}
	identity := uuid.New()
	committed := make(map[int64]model.JobTask)
	var stages []model.TaskType
	sender := &dispatcherSenderStub{call: func(ctx context.Context, attempt RegisteredAttempt) (Response, error) {
		var before model.JobTask
		if err := db.WithContext(ctx).First(&before, attempt.TaskID).Error; err != nil {
			return Response{}, err
		}
		if before.Status != model.TaskRunning || before.AttemptNo != attempt.AttemptNo ||
			!reflect.DeepEqual(before.InputData, attempt.InputData) {
			t.Fatal("sender did not receive the committed stage identity and fixed input")
		}
		report := taskresult.Report{GroupID: attempt.ExecutorGroupID, JobID: attempt.JobID,
			InstanceID: attempt.InstanceID, TaskID: attempt.TaskID, AttemptNo: attempt.AttemptNo,
			ExecutorInstanceID: identity.String(), Outcome: taskresult.OutcomeSuccess,
			Data: json.RawMessage(`{"completed":true}`), Summary: "stage completed before ACK"}
		if attempt.TaskType == model.TaskTypePrepare {
			report.Data = nil
			report.PreparedItems = []json.RawMessage{json.RawMessage(`{"order_id":9007199254740993}`)}
		}
		// The sender boundary deliberately delivers the result before returning
		// its ACK. Persistence is real; this is not a network timing simulation.
		if saved, err := saver.Save(ctx, report); err != nil || !saved.Applied {
			t.Fatalf("save fast stage result: result=%+v err=%v", saved, err)
		}
		var completed model.JobTask
		if err := db.WithContext(ctx).First(&completed, attempt.TaskID).Error; err != nil {
			return Response{}, err
		}
		if completed.Status != model.TaskSucceeded || !reflect.DeepEqual(completed.InputData, before.InputData) ||
			completed.ExecutorInstanceID != nil || completed.ExecutorAddress != nil {
			t.Fatal("fast result changed input or manufactured an ACK binding")
		}
		committed[attempt.TaskID] = completed
		stages = append(stages, attempt.TaskType)
		return Response{Outcome: OutcomeAccepted, ExecutorInstanceID: identity, ExecutorAddress: "127.0.0.1:19001"}, nil
	}}
	dispatcher, err := NewDispatcher(newIntegrationRegistrar(t, db), sender, tasks)
	if err != nil {
		t.Fatal(err)
	}
	assertResponse := func(result DispatchResult, err error) {
		t.Helper()
		if err != nil || !result.Registered || result.Outcome != OutcomeAccepted || result.Applied {
			t.Fatalf("late stage ACK changed a completed Task: result=%+v err=%v", result, err)
		}
		for id, expected := range committed {
			var got model.JobTask
			if err := db.First(&got, id).Error; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("late ACK or a later stage changed Task %d", id)
			}
		}
	}
	assertResponse(dispatcher.DispatchInstance(ctx, instance.ID))

	prepared, err := tasks.FindPreparationResult(ctx, instance.ID)
	if err != nil || prepared == nil {
		t.Fatalf("read saved preparation: result=%+v err=%v", prepared, err)
	}
	generator, err := instancecheck.NewGenerator(registrarTransactionRunner(t, db), nil)
	if err != nil {
		t.Fatal(err)
	}
	if generated, err := generator.Generate(ctx, instance.ID, *prepared); err != nil || !generated {
		t.Fatalf("generate saved preparation: generated=%v err=%v", generated, err)
	}
	dispatchPending := func(stage model.TaskType) {
		t.Helper()
		candidates, err := tasks.ListInstanceDispatchCandidates(ctx, instance.ID, registrarDatabaseTime(t, db), 2)
		if err != nil || len(candidates) != 1 || candidates[0].TaskType != stage {
			t.Fatalf("stage candidate: candidates=%+v err=%v", candidates, err)
		}
		assertResponse(dispatcher.DispatchTask(ctx, candidates[0]))
	}
	dispatchPending(model.TaskTypeShard)
	counts, err := tasks.CountShards(ctx, instance.ID)
	if err != nil || counts != (repository.ShardCounts{Total: 1, Succeeded: 1}) {
		t.Fatalf("finished shard counts: counts=%+v err=%v", counts, err)
	}
	post, err := instancecheck.NewPostCreator(registrarTransactionRunner(t, db), nil)
	if err != nil {
		t.Fatal(err)
	}
	if created, err := post.Create(ctx, instance.ID, counts); err != nil || !created {
		t.Fatalf("create Post from stable counts: created=%v err=%v", created, err)
	}
	dispatchPending(model.TaskTypePost)
	if !reflect.DeepEqual(stages, []model.TaskType{model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost}) || sender.calls.Load() != 3 {
		t.Fatalf("late ACK caused replay or skipped a stage: stages=%v calls=%d", stages, sender.calls.Load())
	}
	var all []model.JobTask
	if err := db.Where("instance_id = ?", instance.ID).Find(&all).Error; err != nil || len(all) != 3 {
		t.Fatalf("stage execution changed the logical Task set: count=%d err=%v", len(all), err)
	}
}
