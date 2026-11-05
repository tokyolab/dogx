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
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestUnboundTargetsUseOneBoundedJoinWithoutEligibilityFiltering(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	var ids []int64
	for range 16 {
		_, task := createResultTask(t, db, job)
		ids = append(ids, task.ID)
	}
	first := readRepositoryTask(t, db, ids[0])
	identity, address := uuid.New(), "127.0.0.1:9001"
	if err := db.Model(&model.JobTask{}).Where("id = ?", first.ID).Updates(map[string]any{
		"task_type": model.TaskTypeShard, "status": model.TaskCanceled,
		"attempt_no": 9, "executor_instance_id": identity, "executor_address": address,
		"attempt_registered_at": nil, "result_data": model.TaskResultData(`{"private":"output"}`), "result_summary": "last attempt",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobInstance{}).Where("id = ?", first.InstanceID).Updates(map[string]any{
		"status": model.InstanceStopping, "started_at": nil, "deadline_at": nil, "execution_config": gorm.Expr("'null'::jsonb"),
	}).Error; err != nil {
		t.Fatal(err)
	}
	last := readRepositoryTask(t, db, ids[15])
	snapshot, err := json.Marshal(map[string]any{
		"schema_version": 1, "execution_mode": 1, "max_retries": 3, "retry_interval_seconds": 10,
		"params":         strings.Repeat("private-business-parameters", 1<<16),
		"processor_name": "private.processor", "post_enabled": true, "timeout_seconds": 900,
		"unneeded_extension": map[string]any{"private": "future business setting"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobInstance{}).Where("id = ?", last.InstanceID).
		Update("execution_config", gorm.Expr("?::jsonb", string(snapshot))).Error; err != nil {
		t.Fatal(err)
	}
	missingParent, orphan := createResultTask(t, db, job)
	if err := db.Delete(&model.JobInstance{}, missingParent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(job).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := repo.FindUnboundTargets(context.Background(), []int64{ids[15], first.ID, first.ID, orphan.ID, orphan.ID + 100000})
	if err != nil || len(rows) != 2 || rows[0].TaskID != first.ID || rows[1].TaskID != ids[15] {
		t.Fatalf("bounded target projection: rows=%+v err=%v", rows, err)
	}
	got := rows[0]
	if got.InstanceID != first.InstanceID || got.AttemptNo != 9 || got.TaskType != model.TaskTypeShard || got.Status != model.TaskCanceled ||
		got.ExecutorInstanceID == nil || *got.ExecutorInstanceID != identity || got.ExecutorAddress == nil || *got.ExecutorAddress != address ||
		got.AttemptRegisteredAt != nil || got.InstanceStatus != model.InstanceStopping || got.StartedAt != nil || got.DeadlineAt != nil || string(got.ExecutionConfig) != "null" {
		t.Fatalf("projection filtered or repaired ineligible facts: %+v", got)
	}
	if rows[1].StartedAt == nil || rows[1].DeadlineAt == nil || rows[1].AttemptRegisteredAt == nil || !json.Valid(rows[1].ExecutionConfig) {
		t.Fatalf("projection lost original parent and attempt times: %+v", rows[1])
	}
	var projected map[string]json.RawMessage
	if err := json.Unmarshal(rows[1].ExecutionConfig, &projected); err != nil {
		t.Fatal(err)
	}
	want := map[string]json.RawMessage{
		"schema_version": json.RawMessage("1"), "execution_mode": json.RawMessage("1"),
		"max_retries": json.RawMessage("3"), "retry_interval_seconds": json.RawMessage("10"),
	}
	if !reflect.DeepEqual(projected, want) || len(rows[1].ExecutionConfig) > 200 {
		t.Fatal("unbound target returned business parameters or changed required snapshot fields")
	}
	if counter.count != 1 {
		t.Fatalf("bounded target read queried per Task: count=%d", counter.count)
	}
	statement := counter.statements[0]
	if !strings.Contains(statement, "INNER JOIN job_instance") || !strings.Contains(statement, " IN (") {
		t.Fatalf("targets did not use one bounded relationship read: %s", statement)
	}
	for _, forbidden := range []string{"result_data", "result_summary", "used_attempts", "job_definition", "FOR UPDATE"} {
		if strings.Contains(statement, forbidden) {
			t.Fatalf("unbound target projection included %s: %s", forbidden, statement)
		}
	}
	rows, err = repo.FindUnboundTargets(context.Background(), ids)
	if err != nil || len(rows) != 16 || counter.count != 2 {
		t.Fatalf("maximum short batch was not read once: len=%d count=%d err=%v", len(rows), counter.count, err)
	}
	rows, err = repo.FindUnboundTargets(context.Background(), []int64{orphan.ID})
	if err != nil || rows == nil || len(rows) != 0 {
		t.Fatalf("missing relationship became a target: rows=%v err=%v", rows, err)
	}
	if err := db.Exec("ALTER TABLE job_instance RENAME TO unavailable_unknown_parent").Error; err != nil {
		t.Fatal(err)
	}
	rows, err = repo.FindUnboundTargets(context.Background(), ids[:2])
	var driverErr *pgconn.PgError
	if rows != nil || !errors.As(err, &driverErr) || driverErr.Code != "42P01" {
		t.Fatalf("failed relationship read became empty success: rows=%v err=%v", rows, err)
	}
}

func TestUnboundTargetsPreserveRequiredJSONTypes(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, task := createResultTask(t, db, job)
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		name string
		raw  string
		want string
	}{
		{"json null", `null`, `null`},
		{"array", `[1, 2]`, `null`},
		{"string", `"snapshot"`, `null`},
		{"number", `42`, `null`},
		{"boolean", `true`, `null`},
		{"missing fields", `{}`, `{"schema_version":null,"execution_mode":null,"max_retries":null,"retry_interval_seconds":null}`},
		{"explicit null", `{"schema_version":1,"execution_mode":1,"max_retries":null,"retry_interval_seconds":null}`,
			`{"schema_version":1,"execution_mode":1,"max_retries":null,"retry_interval_seconds":null}`},
		{"invalid scalar types", `{"schema_version":"1","execution_mode":true,"max_retries":"3","retry_interval_seconds":1.5}`,
			`{"schema_version":"1","execution_mode":true,"max_retries":"3","retry_interval_seconds":1.5}`},
		{"invalid structured types", `{"schema_version":[],"execution_mode":{},"max_retries":[3],"retry_interval_seconds":{"value":10}}`,
			`{"schema_version":[],"execution_mode":{},"max_retries":[3],"retry_interval_seconds":{"value":10}}`},
		{"wide integer", `{"schema_version":1,"execution_mode":1,"max_retries":9223372036854775807,"retry_interval_seconds":10}`,
			`{"schema_version":1,"execution_mode":1,"max_retries":9223372036854775807,"retry_interval_seconds":10}`},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
				Update("execution_config", gorm.Expr("?::jsonb", scenario.raw)).Error; err != nil {
				t.Fatal(err)
			}
			rows, err := repo.FindUnboundTargets(context.Background(), []int64{task.ID})
			if err != nil || len(rows) != 1 {
				t.Fatalf("projection decoded or rejected required JSON: rows=%d err=%v", len(rows), err)
			}
			var got, want any
			gotDecoder := json.NewDecoder(strings.NewReader(string(rows[0].ExecutionConfig)))
			gotDecoder.UseNumber()
			wantDecoder := json.NewDecoder(strings.NewReader(scenario.want))
			wantDecoder.UseNumber()
			if err := gotDecoder.Decode(&got); err != nil {
				t.Fatal(err)
			}
			if err := wantDecoder.Decode(&want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("projection coerced a required field or repaired invalid configuration")
			}
		})
	}
}

func TestOrdinaryUnknownSettlementChangesOnlyResultAndStateFacts(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	processedAt := retryRepositoryTime(t, db)
	for _, scenario := range []struct {
		name string
		used int32
		max  int64
		want model.TaskStatus
	}{
		{"remaining budget", 3, 4, model.TaskRetryWaiting},
		{"exhausted budget", 4, 4, model.TaskFailed},
		{"zero retries", 1, 1, model.TaskFailed},
		{"wide budget", 1<<31 - 1, 1 << 31, model.TaskRetryWaiting},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, task := createUnknownRepositoryTask(t, db, job, processedAt)
			registeredAt := processedAt.Add(-30 * time.Second)
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(map[string]any{
				"attempt_no": 7, "used_attempts": scenario.used, "attempt_registered_at": registeredAt,
				"next_dispatch_at": processedAt.Add(-time.Hour), "result_data": model.TaskResultData(`{"old":true}`),
				"result_summary": "old failure", "finished_at": processedAt.Add(-time.Hour), "end_reason": model.EndReasonExecutionFailed,
			}).Error; err != nil {
				t.Fatal(err)
			}
			before, parentBefore := readRepositoryTask(t, db, task.ID), readCompletionInstance(t, db, instance.ID)
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter, SkipDefaultTransaction: true}))
			if err != nil {
				t.Fatal(err)
			}
			write := UnknownSettlement{TaskID: task.ID, InstanceID: instance.ID, ExpectedAttemptNo: 7, MaxAttempts: scenario.max,
				ProcessedAt: processedAt, ObservationCutoff: registeredAt, RetryAt: processedAt.Add(10 * time.Second)}
			settled, err := repo.SettleOrdinaryUnknown(context.Background(), write)
			if err != nil || settled == nil || *settled != (SettledTask{TaskID: task.ID, InstanceID: instance.ID, AttemptNo: 7, Status: scenario.want}) {
				t.Fatalf("unknown-result branch: settled=%+v err=%v", settled, err)
			}
			if counter.count != 1 || !strings.HasPrefix(counter.statements[0], "UPDATE ") || !strings.Contains(counter.statements[0], "RETURNING") ||
				strings.Contains(counter.statements[0], "clock_timestamp") || strings.Contains(counter.statements[0], "job_instance") {
				t.Fatalf("settlement added a read, parent write or a new clock: %v", counter.statements)
			}
			after := readRepositoryTask(t, db, task.ID)
			if after.Status != scenario.want || after.ResultData != nil || after.ResultSummary == nil || *after.ResultSummary != unknownResultSummary || !after.UpdatedAt.Equal(processedAt) {
				t.Fatalf("unknown result was not recorded with the supplied database time: %+v", after)
			}
			if scenario.want == model.TaskRetryWaiting {
				if after.NextDispatchAt == nil || !after.NextDispatchAt.Equal(write.RetryAt) || after.FinishedAt != nil || after.EndReason != nil {
					t.Fatalf("retry settlement retained terminal fields or changed its delay: %+v", after)
				}
			} else if after.NextDispatchAt != nil || after.FinishedAt == nil || !after.FinishedAt.Equal(processedAt) ||
				after.EndReason == nil || *after.EndReason != model.EndReasonResultUnknown {
				t.Fatalf("final settlement was not marked as result uncertainty: %+v", after)
			}
			expected := before
			expected.Status, expected.ResultData, expected.ResultSummary = after.Status, after.ResultData, after.ResultSummary
			expected.NextDispatchAt, expected.FinishedAt, expected.EndReason, expected.UpdatedAt = after.NextDispatchAt, after.FinishedAt, after.EndReason, after.UpdatedAt
			if !reflect.DeepEqual(expected, after) || !reflect.DeepEqual(parentBefore, readCompletionInstance(t, db, instance.ID)) {
				t.Fatal("settlement changed attempt, budget, original times, binding or parent facts")
			}
			later := write
			later.ProcessedAt, later.ObservationCutoff, later.RetryAt = write.ProcessedAt.Add(time.Second), write.ObservationCutoff.Add(time.Second), write.RetryAt.Add(time.Second)
			if replay, err := repo.SettleOrdinaryUnknown(context.Background(), later); replay != nil || err != nil || !reflect.DeepEqual(after, readRepositoryTask(t, db, task.ID)) {
				t.Fatalf("duplicate settlement refreshed the original outcome: replay=%+v err=%v", replay, err)
			}
		})
	}
}

func TestOrdinaryUnknownSettlementZeroRowsPreserveStoredFacts(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	processedAt := retryRepositoryTime(t, db)
	identity := uuid.New()
	for _, scenario := range []struct {
		name   string
		fields map[string]any
		change func(*UnknownSettlement)
	}{
		{name: "missing Task", change: func(w *UnknownSettlement) { w.TaskID += 100000 }},
		{name: "wrong parent", change: func(w *UnknownSettlement) { w.InstanceID += 100000 }},
		{name: "old attempt", change: func(w *UnknownSettlement) { w.ExpectedAttemptNo++ }},
		{name: "not ordinary", fields: map[string]any{"task_type": model.TaskTypeShard}},
		{name: "waiting", fields: map[string]any{"status": model.TaskWaiting}},
		{name: "retry waiting", fields: map[string]any{"status": model.TaskRetryWaiting}},
		{name: "succeeded", fields: map[string]any{"status": model.TaskSucceeded}},
		{name: "failed", fields: map[string]any{"status": model.TaskFailed}},
		{name: "canceled", fields: map[string]any{"status": model.TaskCanceled}},
		{name: "identity only", fields: map[string]any{"executor_instance_id": identity}},
		{name: "address only", fields: map[string]any{"executor_address": "127.0.0.1:9001"}},
		{name: "complete binding", fields: map[string]any{"executor_instance_id": identity, "executor_address": "127.0.0.1:9001"}},
		{name: "not observed long enough", fields: map[string]any{"attempt_registered_at": processedAt.Add(-30*time.Second + time.Microsecond)}},
		{name: "missing registration time", fields: map[string]any{"attempt_registered_at": nil}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, task := createUnknownRepositoryTask(t, db, job, processedAt)
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Update("attempt_registered_at", processedAt.Add(-30*time.Second)).Error; err != nil {
				t.Fatal(err)
			}
			if len(scenario.fields) != 0 {
				if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(scenario.fields).Error; err != nil {
					t.Fatal(err)
				}
			}
			before := readRepositoryTask(t, db, task.ID)
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
			if err != nil {
				t.Fatal(err)
			}
			write := UnknownSettlement{TaskID: task.ID, InstanceID: instance.ID, ExpectedAttemptNo: 1, MaxAttempts: 4,
				ProcessedAt: processedAt, ObservationCutoff: processedAt.Add(-30 * time.Second), RetryAt: processedAt.Add(10 * time.Second)}
			if scenario.change != nil {
				scenario.change(&write)
			}
			if settled, err := repo.SettleOrdinaryUnknown(context.Background(), write); settled != nil || err != nil || counter.count != 1 {
				t.Fatalf("ineligible settlement: settled=%+v err=%v queries=%d", settled, err, counter.count)
			}
			if !reflect.DeepEqual(before, readRepositoryTask(t, db, task.ID)) {
				t.Fatal("zero-row settlement changed stored Task facts")
			}
		})
	}
}

func createUnknownRepositoryTask(t *testing.T, db *gorm.DB, job *model.JobDefinition, processedAt time.Time) (model.JobInstance, model.JobTask) {
	t.Helper()
	instance, task := createResultTask(t, db, job)
	startedAt, deadlineAt := processedAt.Add(-time.Minute), processedAt.Add(14*time.Minute)
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
		Updates(map[string]any{"started_at": startedAt, "deadline_at": deadlineAt}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).
		Updates(map[string]any{"started_at": startedAt, "attempt_registered_at": startedAt}).Error; err != nil {
		t.Fatal(err)
	}
	return readCompletionInstance(t, db, instance.ID), readRepositoryTask(t, db, task.ID)
}
