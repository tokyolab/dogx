//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func createResultTask(t *testing.T, db *gorm.DB, job *model.JobDefinition) (model.JobInstance, model.JobTask) {
	t.Helper()
	started := time.Date(2000, 1, 2, 3, 4, 5, 0, time.UTC)
	deadline := started.Add(15 * time.Minute)
	requestID, operatorID := uuid.New(), int64(1)
	instance := model.JobInstance{JobID: job.ID, JobName: job.Name, ExecutorGroupID: job.ExecutorGroupID,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: job.ProcessorName,
			ExecutionMode: model.ExecutionModeNormal, MaxRetries: 3, RetryIntervalSeconds: 10, TimeoutSeconds: 900},
		Status: model.InstanceRunning, StartedAt: &started, DeadlineAt: &deadline, CreatedAt: started, UpdatedAt: started}
	if err := db.Create(&instance).Error; err != nil {
		t.Fatal(err)
	}
	task := model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeOrdinary,
		Status: model.TaskRunning, AttemptNo: 1, UsedAttempts: 1,
		AttemptRegisteredAt: &started, StartedAt: &started, CreatedAt: started, UpdatedAt: started}
	if err := db.Create(&task).Error; err != nil {
		t.Fatal(err)
	}
	return instance, task
}

func resultRepositoryFixture(t *testing.T) (*gorm.DB, *model.JobDefinition) {
	t.Helper()
	db, _ := testutil.OpenMigratedPostgres(t)
	group := createReferenceTestGroup(t, db, "result-repository")
	job := referenceTestJob(group.ID)
	if err := db.Create(job).Error; err != nil {
		t.Fatal(err)
	}
	return db, job
}

func TestTaskResultTargetUsesOneJoinAndKeepsSnapshotRaw(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, task := createResultTask(t, db, job)
	// A JSON scalar is valid JSONB but cannot decode into ExecutionConfig. The
	// projection must still expose stale-attempt facts for the caller to classify.
	raw := `"unusable frozen snapshot"`
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
		Update("execution_config", gorm.Expr("?::jsonb", raw)).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	target, err := repo.FindResultTarget(context.Background(), task.ID)
	if err != nil || target == nil || target.TaskID != task.ID || target.InstanceID != instance.ID ||
		target.JobID != job.ID || target.ExecutorGroupID != job.ExecutorGroupID || target.AttemptNo != 1 ||
		target.UsedAttempts != 1 || target.TaskType != model.TaskTypeOrdinary || target.Status != model.TaskRunning ||
		target.ExecutorInstanceID != nil || string(target.ExecutionConfig) != raw {
		t.Fatalf("unexpected target: %+v err=%v", target, err)
	}
	if counter.count != 1 || !strings.Contains(counter.statements[0], "INNER JOIN job_instance") ||
		strings.Contains(counter.statements[0], "job_definition") || strings.Contains(counter.statements[0], "deadline_at") {
		t.Fatalf("target lookup exceeded its projection: %v", counter.statements)
	}
	if err := db.Where("id = ?", instance.ID).Delete(&model.JobInstance{}).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{task.ID, task.ID + 1000} {
		if target, err := repo.FindResultTarget(context.Background(), id); target != nil || err != nil {
			t.Fatalf("missing target/parent: %+v err=%v", target, err)
		}
	}
}

func TestTaskResultRepositoryPersistsEachOutcomeWithOneClock(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	for _, scenario := range []struct {
		name   string
		status model.TaskStatus
		data   model.TaskData
	}{
		{"success without data", model.TaskSucceeded, nil},
		{"success JSON null", model.TaskSucceeded, model.TaskData(`null`)},
		{"success large integer", model.TaskSucceeded, model.TaskData(`9007199254740993`)},
		{"retry ignores malformed data", model.TaskRetryWaiting, model.TaskData(`{`)},
		{"final failure ignores data", model.TaskFailed, model.TaskData(`{"ignored":true}`)},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, task := createResultTask(t, db, job)
			// Old output and nullable fields must be explicitly replaced, including
			// an empty summary and SQL NULL, rather than omitted as Go zero values.
			oldTime := task.CreatedAt
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(map[string]any{
				"result_data": model.TaskData(`{"old":true}`), "result_summary": "old summary",
				"next_dispatch_at": oldTime, "finished_at": oldTime, "end_reason": "old reason",
			}).Error; err != nil {
				t.Fatal(err)
			}
			before := readRepositoryTask(t, db, task.ID)
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
			if err != nil {
				t.Fatal(err)
			}
			write := resultWriteFixture()
			write.TaskID, write.InstanceID, write.Status, write.Data, write.Summary = task.ID, instance.ID, scenario.status, scenario.data, ""
			write.RetryIntervalSeconds = 11
			var beforeWrite time.Time
			if err := db.Raw("SELECT clock_timestamp()").Scan(&beforeWrite).Error; err != nil {
				t.Fatal(err)
			}
			if applied, err := repo.SaveResult(context.Background(), write); !applied || err != nil {
				t.Fatalf("save result: applied=%v err=%v", applied, err)
			}
			if counter.count != 1 || !strings.HasPrefix(counter.statements[0], "UPDATE ") ||
				strings.Count(counter.statements[0], "clock_timestamp()") != 1 {
				t.Fatalf("save did not use one update/clock: %v", counter.statements)
			}
			after := readRepositoryTask(t, db, task.ID)
			if after.Status != scenario.status || after.ResultSummary == nil || *after.ResultSummary != "" || after.UpdatedAt.Before(beforeWrite) {
				t.Fatalf("incorrect state/summary/time: %+v", after)
			}
			if scenario.status == model.TaskRetryWaiting {
				if after.ResultData != nil || after.FinishedAt != nil || after.EndReason != nil || after.NextDispatchAt == nil ||
					!after.NextDispatchAt.Equal(after.UpdatedAt.Add(11*time.Second)) {
					t.Fatalf("incorrect retry outcome: %+v", after)
				}
			} else {
				wantReason := model.EndReasonSuccess
				if scenario.status == model.TaskFailed {
					wantReason = model.EndReasonExecutionFailed
				}
				if after.FinishedAt == nil || !after.FinishedAt.Equal(after.UpdatedAt) || after.NextDispatchAt != nil ||
					after.EndReason == nil || *after.EndReason != wantReason {
					t.Fatalf("incorrect terminal outcome: %+v", after)
				}
				if scenario.status == model.TaskSucceeded {
					if string(after.ResultData) != string(scenario.data) || (after.ResultData == nil) != (scenario.data == nil) {
						t.Fatalf("result JSON changed: got=%s want=%s", after.ResultData, scenario.data)
					}
				} else if after.ResultData != nil {
					t.Fatal("failed outcome retained business output")
				}
			}
			expected := before
			expected.Status, expected.ResultData, expected.ResultSummary = after.Status, after.ResultData, after.ResultSummary
			expected.EndReason, expected.FinishedAt, expected.NextDispatchAt, expected.UpdatedAt = after.EndReason, after.FinishedAt, after.NextDispatchAt, after.UpdatedAt
			if !reflect.DeepEqual(expected, after) {
				t.Fatalf("result changed unrelated execution facts: before=%+v after=%+v", before, after)
			}
			counter.count, counter.statements = 0, nil
			if applied, err := repo.SaveResult(context.Background(), write); applied || err != nil || counter.count != 1 {
				t.Fatalf("duplicate result: applied=%v err=%v statements=%d", applied, err, counter.count)
			}
			if duplicate := readRepositoryTask(t, db, task.ID); !reflect.DeepEqual(after, duplicate) {
				t.Fatal("duplicate result changed committed facts")
			}
		})
	}
}

func TestTaskResultRepositoryPreservesFixedStageInput(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []struct {
		name  string
		kind  model.TaskType
		input model.TaskData
	}{
		{"shard", model.TaskTypeShard, model.TaskData(`{"fixed":9007199254740993}`)},
		{"post", model.TaskTypePost, model.TaskData(`{"total":3,"succeeded":2,"failed":1}`)},
	} {
		t.Run(stage.name, func(t *testing.T) {
			for _, status := range []model.TaskStatus{model.TaskSucceeded, model.TaskRetryWaiting, model.TaskFailed} {
				instance, task := createResultTask(t, db, job)
				instance.ExecutionConfig.ExecutionMode = model.ExecutionModeSharded
				instance.ExecutionConfig.PostEnabled = true
				if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("execution_config", instance.ExecutionConfig).Error; err != nil {
					t.Fatal(err)
				}
				if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(map[string]any{
					"task_type": stage.kind, "input_data": stage.input,
				}).Error; err != nil {
					t.Fatal(err)
				}
				before := readRepositoryTask(t, db, task.ID)
				write := resultWriteFixture()
				write.TaskID, write.InstanceID, write.Status = task.ID, instance.ID, status
				write.Data = model.TaskData(`{"output":true}`)
				write.RetryIntervalSeconds = 10
				if applied, err := repo.SaveResult(context.Background(), write); err != nil || !applied {
					t.Fatalf("save stage result: status=%d applied=%v err=%v", status, applied, err)
				}
				stored := readRepositoryTask(t, db, task.ID)
				if stored.Status != status || !reflect.DeepEqual(stored.InputData, before.InputData) {
					t.Fatalf("result changed fixed input: status=%d input=%s", stored.Status, stored.InputData)
				}
			}
		})
	}
}

func TestTaskResultRepositoryRechecksIdentityAndAllowsStoppedParent(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	otherIdentity := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	for _, scenario := range []struct {
		name   string
		fields map[string]any
		apply  bool
	}{
		{"unbound", nil, true},
		{"matching binding", map[string]any{"executor_instance_id": resultWriteFixture().ExecutorInstanceID, "executor_address": "127.0.0.1:9000"}, true},
		{"address only", map[string]any{"executor_address": "127.0.0.1:9000"}, true},
		{"conflicting binding", map[string]any{"executor_instance_id": otherIdentity}, false},
		{"new attempt", map[string]any{"attempt_no": 2}, false},
		{"retry waiting", map[string]any{"status": model.TaskRetryWaiting}, false},
		{"canceled", map[string]any{"status": model.TaskCanceled}, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, task := createResultTask(t, db, job)
			repo, err := NewJobTaskRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			if target, err := repo.FindResultTarget(context.Background(), task.ID); target == nil || err != nil {
				t.Fatalf("initial target: %+v err=%v", target, err)
			}
			// The parent deadline is already past; a concurrent stopping decision
			// cannot invalidate an otherwise eligible child result.
			if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
				Updates(map[string]any{"status": model.InstanceStopping, "end_reason": "manual_stop"}).Error; err != nil {
				t.Fatal(err)
			}
			if len(scenario.fields) > 0 {
				if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(scenario.fields).Error; err != nil {
					t.Fatal(err)
				}
			}
			before := readRepositoryTask(t, db, task.ID)
			write := resultWriteFixture()
			write.TaskID, write.InstanceID = task.ID, instance.ID
			if applied, err := repo.SaveResult(context.Background(), write); applied != scenario.apply || err != nil {
				t.Fatalf("conditional result: applied=%v err=%v", applied, err)
			}
			after := readRepositoryTask(t, db, task.ID)
			if !scenario.apply && !reflect.DeepEqual(before, after) {
				t.Fatal("stale result overwrote current facts")
			}
			if !reflect.DeepEqual(before.ExecutorInstanceID, after.ExecutorInstanceID) || !reflect.DeepEqual(before.ExecutorAddress, after.ExecutorAddress) {
				t.Fatal("result filled or replaced executor binding")
			}
			var parent model.JobInstance
			if err := db.Where("id = ?", instance.ID).Take(&parent).Error; err != nil {
				t.Fatal(err)
			}
			if parent.Status != model.InstanceStopping || parent.EndReason == nil || *parent.EndReason != "manual_stop" {
				t.Fatal("child result changed the parent stopping decision")
			}
		})
	}
}

func TestTaskResultDataProbeChecksPostgresRepresentationAndKeepsErrors(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		data string
		want bool
	}{
		{`null`, true}, {`9007199254740993`, true}, {`"ordinary text"`, true},
		{`"\u0000"`, false}, {`"\ud800"`, false}, {`1e1000000`, false}, {`{`, false},
	} {
		valid, err := repo.CheckResultData(context.Background(), model.TaskData(scenario.data))
		if err != nil || valid != scenario.want {
			t.Fatalf("JSONB probe: data=%q valid=%v err=%v", scenario.data, valid, err)
		}
	}
	instance, task := createResultTask(t, db, job)
	write := resultWriteFixture()
	write.TaskID, write.InstanceID, write.Data = task.ID, instance.ID, model.TaskData(`"\u0000"`)
	if !json.Valid(write.Data) {
		t.Fatal("fixture must be syntactically valid JSON")
	}
	if applied, err := repo.SaveResult(context.Background(), write); applied || err == nil {
		t.Fatalf("unsupported JSONB was saved: applied=%v err=%v", applied, err)
	}
	// The failed independent write must leave the root connection usable for an
	// evidence probe, rather than hiding the probe in an aborted transaction.
	if valid, err := repo.CheckResultData(context.Background(), write.Data); valid || err != nil {
		t.Fatalf("probe after failed write: valid=%v err=%v", valid, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if valid, err := repo.CheckResultData(ctx, model.TaskData(`null`)); valid || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe: valid=%v err=%v", valid, err)
	}
	if target, err := repo.FindResultTarget(ctx, task.ID); target != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled target read: %+v err=%v", target, err)
	}
	write.Data = nil
	if applied, err := repo.SaveResult(ctx, write); applied || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled result: applied=%v err=%v", applied, err)
	}
	if err := db.Exec("DROP TABLE job_task").Error; err != nil {
		t.Fatal(err)
	}
	if applied, err := repo.SaveResult(context.Background(), write); applied || err == nil {
		t.Fatalf("missing table reported success: applied=%v err=%v", applied, err)
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42P01" {
			t.Fatalf("write lost PostgreSQL cause: %v", err)
		}
	}
}
