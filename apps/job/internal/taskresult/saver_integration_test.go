//go:build integration

package taskresult

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestSaverPersistsOrdinaryOutcomeBranches(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	for _, tc := range []struct {
		name       string
		outcome    Outcome
		maxRetries int32
		used       int32
		want       model.TaskStatus
		endReason  string
	}{
		{"success", OutcomeSuccess, 2, 1, model.TaskSucceeded, model.EndReasonSuccess},
		{"retry", OutcomeFailure, 2, 1, model.TaskRetryWaiting, ""},
		{"exhausted", OutcomeFailure, 2, 3, model.TaskFailed, model.EndReasonExecutionFailed},
		{"zero_retries", OutcomeFailure, 0, 1, model.TaskFailed, model.EndReasonExecutionFailed},
		{"large_retry_budget", OutcomeFailure, math.MaxInt32, math.MaxInt32, model.TaskRetryWaiting, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSaverIntegrationFixture(t, db, func(instance *model.JobInstance, task *model.JobTask) {
				instance.ExecutionConfig.MaxRetries = tc.maxRetries
				task.UsedAttempts = tc.used
				// Dispatch identity may exceed consumed opportunities after BUSY.
				task.AttemptNo = int64(tc.used) + 10
			})
			report := fixture.report
			report.Outcome, report.Summary = tc.outcome, "current result"
			if tc.outcome == OutcomeFailure {
				// Failure never adopts output, including malformed unused bytes.
				report.Data = json.RawMessage(`{`)
				report.PreparedItems = []json.RawMessage{json.RawMessage(`not JSON`)}
			}
			var hints []int64
			saver := integrationSaver(t, db, WithInstanceHint(func(id int64) {
				hints = append(hints, id)
			}))
			before := saverDatabaseTime(t, db)
			result, err := saver.Save(context.Background(), report)
			after := saverDatabaseTime(t, db)
			if err != nil || !result.Applied || result.Reason != "saved" {
				t.Fatalf("save outcome: result=%+v error=%v", result, err)
			}
			if !reflect.DeepEqual(hints, []int64{fixture.instance.ID}) {
				t.Fatalf("committed result hints=%v", hints)
			}
			stored := loadSaverTask(t, db, report.TaskID)
			if stored.Status != tc.want || stored.ResultSummary == nil || *stored.ResultSummary != report.Summary {
				t.Fatalf("unexpected saved outcome: %+v", stored)
			}
			if stored.UpdatedAt.Before(before) || stored.UpdatedAt.After(after) {
				t.Fatal("result update time is outside the database processing interval")
			}
			if tc.want == model.TaskRetryWaiting {
				if stored.FinishedAt != nil || stored.EndReason != nil || stored.NextDispatchAt == nil ||
					!stored.NextDispatchAt.Equal(stored.UpdatedAt.Add(7*time.Second)) {
					t.Fatalf("invalid retry scheduling facts: %+v", stored)
				}
			} else if stored.NextDispatchAt != nil || stored.FinishedAt == nil || !stored.FinishedAt.Equal(stored.UpdatedAt) ||
				stored.EndReason == nil || *stored.EndReason != tc.endReason {
				t.Fatalf("invalid terminal facts: %+v", stored)
			}
			if tc.outcome == OutcomeFailure && stored.ResultData != nil {
				t.Fatal("failure saved unused business output")
			}
			if stored.AttemptNo != fixture.task.AttemptNo || stored.UsedAttempts != fixture.task.UsedAttempts ||
				!reflect.DeepEqual(stored.StartedAt, fixture.task.StartedAt) ||
				!reflect.DeepEqual(stored.AttemptRegisteredAt, fixture.task.AttemptRegisteredAt) ||
				!stored.CreatedAt.Equal(fixture.task.CreatedAt) || stored.ExecutorInstanceID != nil || stored.ExecutorAddress != nil {
				t.Fatal("result persistence changed registration facts, execution budget or binding")
			}
			if got := loadSaverInstance(t, db, fixture.instance.ID); !reflect.DeepEqual(got, fixture.instance) {
				t.Fatal("Task result persistence changed the parent Instance")
			}
		})
	}
}

func TestSaverPreservesJSONOutputSemantics(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	for _, tc := range []struct {
		name string
		data json.RawMessage
		want string
	}{
		{"absent", nil, ""},
		{"json_null", json.RawMessage(`null`), "null"},
		{"large_integer", json.RawMessage(`9007199254740993`), "9007199254740993"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSaverIntegrationFixture(t, db, func(_ *model.JobInstance, task *model.JobTask) {
				task.ResultData = model.TaskResultData(`{"previous":true}`)
			})
			report := fixture.report
			report.Data, report.Summary = tc.data, ""
			result, err := integrationSaver(t, db).Save(context.Background(), report)
			if err != nil || !result.Applied {
				t.Fatalf("save JSON output: result=%+v error=%v", result, err)
			}
			stored := loadSaverTask(t, db, report.TaskID)
			if tc.data == nil && stored.ResultData != nil || string(stored.ResultData) != tc.want {
				t.Fatalf("JSON storage changed presence or precision: got=%q want=%q", stored.ResultData, tc.want)
			}
			if stored.ResultSummary == nil || *stored.ResultSummary != "" {
				t.Fatal("successful empty summary retained an earlier result description")
			}
		})
	}
}

func TestSaverPreservesMatchingExecutorBinding(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	fixture := newSaverIntegrationFixture(t, db, func(_ *model.JobInstance, task *model.JobTask) {
		identity, address := uuid.New(), "127.0.0.1:19090"
		task.ExecutorInstanceID, task.ExecutorAddress = &identity, &address
	})
	report := fixture.report
	report.ExecutorInstanceID = fixture.task.ExecutorInstanceID.String()
	result, err := integrationSaver(t, db).Save(context.Background(), report)
	if err != nil || !result.Applied {
		t.Fatalf("matching bound executor: result=%+v error=%v", result, err)
	}
	stored := loadSaverTask(t, db, fixture.task.ID)
	if !reflect.DeepEqual(stored.ExecutorInstanceID, fixture.task.ExecutorInstanceID) ||
		!reflect.DeepEqual(stored.ExecutorAddress, fixture.task.ExecutorAddress) {
		t.Fatal("result persistence changed the ACK's executor binding")
	}
}

func TestSaverUsesFrozenConfig(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	fixture := newSaverIntegrationFixture(t, db, nil)
	job := model.JobDefinition{Name: "edited job", ExecutorGroupID: fixture.instance.ExecutorGroupID + 1,
		ProcessorName: "edited.processor", ExecutionMode: model.ExecutionModeSharded, PostEnabled: true,
		Params: "edited parameters", CronExpr: "0 * * * * *", MaxRetries: 0,
		RetryIntervalSeconds: 99, TimeoutSeconds: 1}
	job.ID = fixture.instance.JobID
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	report := fixture.report
	report.Outcome = OutcomeFailure
	result, err := integrationSaver(t, db).Save(context.Background(), report)
	if err != nil || !result.Applied {
		t.Fatalf("save with edited current Job: result=%+v error=%v", result, err)
	}
	stored := loadSaverTask(t, db, report.TaskID)
	if stored.Status != model.TaskRetryWaiting || stored.NextDispatchAt == nil || !stored.NextDispatchAt.Equal(stored.UpdatedAt.Add(7*time.Second)) {
		t.Fatalf("result used current Job instead of frozen retry policy: %+v", stored)
	}
}

func TestSaverDiscardsStaleReportsBeforeDecodingSnapshot(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	for _, tc := range []struct {
		name   string
		reason string
		mutate func(*model.JobTask, *Report)
	}{
		{"old_attempt", "stale_attempt", func(_ *model.JobTask, r *Report) { r.AttemptNo-- }},
		{"retry_waiting", "task_closed", func(task *model.JobTask, _ *Report) { task.Status = model.TaskRetryWaiting }},
		{"succeeded", "task_closed", func(task *model.JobTask, _ *Report) { task.Status = model.TaskSucceeded }},
		{"canceled", "task_closed", func(task *model.JobTask, _ *Report) { task.Status = model.TaskCanceled }},
		{"different_executor", "executor_mismatch", func(task *model.JobTask, _ *Report) {
			id := uuid.New()
			task.ExecutorInstanceID = &id
		}},
		{"wrong_group", "ownership_mismatch", func(_ *model.JobTask, r *Report) { r.GroupID++ }},
		{"wrong_job", "ownership_mismatch", func(_ *model.JobTask, r *Report) { r.JobID++ }},
		{"wrong_instance", "ownership_mismatch", func(_ *model.JobTask, r *Report) { r.InstanceID++ }},
		{"missing_task", "missing_target", func(_ *model.JobTask, r *Report) { r.TaskID += 1000000 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSaverIntegrationFixture(t, db, nil)
			report, task := fixture.report, fixture.task
			tc.mutate(&task, &report)
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(map[string]any{
				"status": task.Status, "executor_instance_id": task.ExecutorInstanceID,
			}).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&model.JobInstance{}).Where("id = ?", fixture.instance.ID).
				Update("execution_config", gorm.Expr("?::jsonb", `{"schema_version":"broken"}`)).Error; err != nil {
				t.Fatal(err)
			}
			before := loadSaverTask(t, db, task.ID)
			result, err := integrationSaver(t, db).Save(context.Background(), report)
			if err != nil || result.Applied || result.Reason != tc.reason {
				t.Fatalf("stale result was retained because of an unused snapshot: result=%+v error=%v", result, err)
			}
			if got := loadSaverTask(t, db, task.ID); !reflect.DeepEqual(got, before) {
				t.Fatal("discarded report changed persisted Task facts")
			}
		})
	}
}

func TestSaverRetainsReportsForUnknownStateOrNeededBrokenConfig(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	for _, tc := range []struct {
		name   string
		status model.TaskStatus
		config string
	}{
		{"unknown_state", 99, ""},
		{"malformed_snapshot", model.TaskRunning, `{"schema_version":"broken"}`},
		{"unknown_snapshot_version", model.TaskRunning, `{"schema_version":99,"execution_mode":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSaverIntegrationFixture(t, db, func(_ *model.JobInstance, task *model.JobTask) { task.Status = tc.status })
			if tc.config != "" {
				if err := db.Model(&model.JobInstance{}).Where("id = ?", fixture.instance.ID).
					Update("execution_config", gorm.Expr("?::jsonb", tc.config)).Error; err != nil {
					t.Fatal(err)
				}
			}
			hints := 0
			result, err := integrationSaver(t, db, WithInstanceHint(func(int64) { hints++ })).Save(context.Background(), fixture.report)
			if err == nil || result.Applied || hints != 0 {
				t.Fatalf("platform inconsistency was discarded or hinted: result=%+v error=%v hints=%d", result, err, hints)
			}
			if got := loadSaverTask(t, db, fixture.task.ID); !reflect.DeepEqual(got, fixture.task) {
				t.Fatal("failed processing changed the Task")
			}
		})
	}
}

func TestSaverAcceptsRunningChildOfStoppingOrExpiredParent(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	for _, state := range []model.InstanceStatus{model.InstanceRunning, model.InstanceStopping} {
		t.Run(fmt.Sprintf("parent_%d", state), func(t *testing.T) {
			fixture := newSaverIntegrationFixture(t, db, func(instance *model.JobInstance, _ *model.JobTask) {
				instance.Status = state
				expired := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
				instance.DeadlineAt = &expired
			})
			result, err := integrationSaver(t, db).Save(context.Background(), fixture.report)
			if err != nil || !result.Applied {
				t.Fatalf("parent state or deadline rejected an effective child result: result=%+v error=%v", result, err)
			}
			if got := loadSaverInstance(t, db, fixture.instance.ID); !reflect.DeepEqual(got, fixture.instance) {
				t.Fatal("saving a child result changed the parent's stopping or expiry decision")
			}
		})
	}
}

func TestSaverDoesNotOverwriteCompletedOrNewAttempt(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	fixture := newSaverIntegrationFixture(t, db, nil)
	saver := integrationSaver(t, db)
	result, err := saver.Save(context.Background(), fixture.report)
	if err != nil || !result.Applied {
		t.Fatalf("initial result: result=%+v error=%v", result, err)
	}
	stored := loadSaverTask(t, db, fixture.task.ID)
	for _, outcome := range []Outcome{OutcomeSuccess, OutcomeFailure} {
		repeated := fixture.report
		repeated.Outcome, repeated.Summary = outcome, "must not replace the first result"
		result, err = saver.Save(context.Background(), repeated)
		if err != nil || result.Applied || result.Reason != "task_closed" {
			t.Fatalf("repeat or contrary result: result=%+v error=%v", result, err)
		}
		if got := loadSaverTask(t, db, fixture.task.ID); !reflect.DeepEqual(got, stored) {
			t.Fatal("later result overwrote the completed attempt")
		}
	}
	retrying := newSaverIntegrationFixture(t, db, nil)
	retrying.report.Outcome = OutcomeFailure
	result, err = saver.Save(context.Background(), retrying.report)
	if err != nil || !result.Applied {
		t.Fatalf("result preceding retry: result=%+v error=%v", result, err)
	}
	// Model authorization after retry-waiting; the old report keeps its identity.
	if err := db.Model(&model.JobTask{}).Where("id = ?", retrying.task.ID).Updates(map[string]any{
		"status": model.TaskRunning, "attempt_no": retrying.task.AttemptNo + 1,
		"used_attempts": retrying.task.UsedAttempts + 1, "next_dispatch_at": nil,
		"result_data": nil, "finished_at": nil, "end_reason": nil,
	}).Error; err != nil {
		t.Fatal(err)
	}
	newAttempt := loadSaverTask(t, db, retrying.task.ID)
	result, err = saver.Save(context.Background(), retrying.report)
	if err != nil || result.Applied || result.Reason != "stale_attempt" {
		t.Fatalf("old report was applied to the new attempt: result=%+v error=%v", result, err)
	}
	if got := loadSaverTask(t, db, retrying.task.ID); !reflect.DeepEqual(got, newAttempt) {
		t.Fatal("old report overwrote a newer attempt")
	}
}

func TestSaverProbesPostgresInvalidJSONB(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	for _, tc := range []struct {
		name string
		data json.RawMessage
	}{
		{"nul_character", json.RawMessage(`"\u0000"`)},
		{"unpaired_surrogate", json.RawMessage(`"\ud800"`)},
		{"numeric_overflow", json.RawMessage(`1e1000000`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !json.Valid(tc.data) {
				t.Fatal("fixture must be valid JSON before testing PostgreSQL storage restrictions")
			}
			var valid bool
			if err := db.Raw("SELECT pg_input_is_valid(?, 'jsonb')", string(tc.data)).Scan(&valid).Error; err != nil {
				t.Fatal(err)
			}
			if valid {
				t.Fatal("fixture does not reproduce a PostgreSQL JSONB restriction")
			}
			fixture := newSaverIntegrationFixture(t, db, nil)
			report := fixture.report
			report.Data = tc.data
			hints := 0
			result, err := integrationSaver(t, db, WithInstanceHint(func(int64) { hints++ })).Save(context.Background(), report)
			if err != nil || result.Applied || result.Reason != "invalid_output" || hints != 0 {
				t.Fatalf("independently invalid output: result=%+v error=%v hints=%d", result, err, hints)
			}
			if got := loadSaverTask(t, db, fixture.task.ID); !reflect.DeepEqual(got, fixture.task) {
				t.Fatal("rejected JSONB output changed Task state")
			}
		})
	}
}

func TestSaverRetainsValidOutputOnMatchingSQLStateFailure(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	// A trigger can raise the same SQLSTATE as invalid JSONB. The classification
	// must independently check the reported data rather than trust that code.
	if err := db.Exec(`CREATE FUNCTION fail_result_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION USING ERRCODE = TG_ARGV[0], MESSAGE = 'synthetic result storage failure';
 RETURN NEW;
END $$`).Error; err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"22P05", "22P02", "22003"} {
		t.Run(code, func(t *testing.T) {
			fixture := newSaverIntegrationFixture(t, db, nil)
			if err := db.Exec(fmt.Sprintf("CREATE TRIGGER reject_result BEFORE UPDATE ON job_task FOR EACH ROW EXECUTE FUNCTION fail_result_write('%s')", code)).Error; err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Exec("DROP TRIGGER reject_result ON job_task").Error; err != nil {
					t.Errorf("drop failure trigger: %v", err)
				}
			})
			hints := 0
			result, err := integrationSaver(t, db, WithInstanceHint(func(int64) { hints++ })).Save(context.Background(), fixture.report)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != code || result.Applied || hints != 0 {
				t.Fatalf("valid output was discarded on an unrelated storage error: result=%+v error=%v hints=%d", result, err, hints)
			}
			if got := loadSaverTask(t, db, fixture.task.ID); !reflect.DeepEqual(got, fixture.task) {
				t.Fatal("failed result update changed persisted Task facts")
			}
		})
	}
}

type saverIntegrationFixture struct {
	instance model.JobInstance
	task     model.JobTask
	report   Report
}

func newSaverIntegrationFixture(t *testing.T, db *gorm.DB, mutate func(*model.JobInstance, *model.JobTask)) saverIntegrationFixture {
	t.Helper()
	requestID, operatorID := uuid.New(), int64(1)
	started := time.Date(2026, time.January, 1, 1, 0, 0, 123456000, time.UTC)
	deadline := started.Add(15 * time.Minute)
	previousSummary := "previous attempt description"
	instance := model.JobInstance{JobID: 11, JobName: "frozen result job", ExecutorGroupID: 17,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "system.cleanup",
			ExecutionMode: model.ExecutionModeNormal, Params: "frozen parameters", MaxRetries: 2,
			RetryIntervalSeconds: 7, TimeoutSeconds: 900},
		Status: model.InstanceRunning, StartedAt: &started, DeadlineAt: &deadline, CreatedAt: started, UpdatedAt: started}
	task := model.JobTask{TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning, AttemptNo: 11, UsedAttempts: 1,
		StartedAt: &started, AttemptRegisteredAt: &started, ResultSummary: &previousSummary, CreatedAt: started, UpdatedAt: started}
	if mutate != nil {
		mutate(&instance, &task)
	}
	if err := db.Create(&instance).Error; err != nil {
		t.Fatal(err)
	}
	task.InstanceID = instance.ID
	if err := db.Create(&task).Error; err != nil {
		t.Fatal(err)
	}
	instance, task = loadSaverInstance(t, db, instance.ID), loadSaverTask(t, db, task.ID)
	return saverIntegrationFixture{instance: instance, task: task, report: Report{
		GroupID: instance.ExecutorGroupID, JobID: instance.JobID, InstanceID: instance.ID, TaskID: task.ID,
		AttemptNo: task.AttemptNo, ExecutorInstanceID: uuid.NewString(), Outcome: OutcomeSuccess,
		Summary: "finished", Data: json.RawMessage(`{"done":true}`),
	}}
}

func integrationSaver(t *testing.T, db *gorm.DB, options ...Option) *Saver {
	t.Helper()
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	saver, err := NewSaver(tasks, options...)
	if err != nil {
		t.Fatal(err)
	}
	return saver
}

func loadSaverTask(t *testing.T, db *gorm.DB, id int64) model.JobTask {
	t.Helper()
	var task model.JobTask
	if err := db.Where("id = ?", id).Take(&task).Error; err != nil {
		t.Fatal(err)
	}
	return task
}

func loadSaverInstance(t *testing.T, db *gorm.DB, id int64) model.JobInstance {
	t.Helper()
	var instance model.JobInstance
	if err := db.Where("id = ?", id).Take(&instance).Error; err != nil {
		t.Fatal(err)
	}
	return instance
}

func saverDatabaseTime(t *testing.T, db *gorm.DB) time.Time {
	t.Helper()
	var now time.Time
	if err := db.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		t.Fatal(err)
	}
	return now
}
