//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestStageQueriesReadOnlyRelevantSingletonFacts(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, _ := createResultTask(t, db, job)
	summary := "private stage output"
	tasks := []model.JobTask{
		{InstanceID: instance.ID, TaskType: model.TaskTypePrepare, Status: model.TaskRunning,
			ResultSummary: &summary, ResultData: model.TaskData(`[9007199254740993]`)},
		{InstanceID: instance.ID, TaskType: model.TaskTypePost, Status: model.TaskFailed,
			ResultSummary: &summary, InputData: model.TaskData(`{"total":1,"succeeded":1,"failed":0}`), ResultData: model.TaskData(`{"private":true}`)},
		{InstanceID: instance.ID, TaskType: model.TaskTypeShard, Status: model.TaskFailed, ResultSummary: &summary},
	}
	if err := db.Create(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, state := range []model.TaskStatus{model.TaskWaiting, model.TaskRunning, model.TaskRetryWaiting, model.TaskSucceeded, model.TaskFailed, model.TaskCanceled, 99} {
		if err := db.Model(&model.JobTask{}).Where("id = ?", tasks[0].ID).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
		counter.count, counter.statements = 0, nil
		facts, err := repo.FindStages(ctx, instance.ID)
		if err != nil || facts == nil || facts.Preparation == nil || facts.Post == nil ||
			facts.Preparation.ID != tasks[0].ID || facts.Preparation.TaskType != model.TaskTypePrepare || facts.Preparation.Status != state ||
			facts.Post.ID != tasks[1].ID || facts.Post.TaskType != model.TaskTypePost || facts.Post.Status != model.TaskFailed || facts.Post.ResultSummary != nil {
			t.Fatalf("minimal stage facts changed identities/states: %+v err=%v", facts, err)
		}
		if state == model.TaskFailed {
			if facts.Preparation.ResultSummary == nil || *facts.Preparation.ResultSummary != summary {
				t.Fatalf("failed preparation lost its summary: %+v", facts.Preparation)
			}
		} else if facts.Preparation.ResultSummary != nil {
			t.Fatal("nonfailed preparation loaded an unused summary")
		}
		if counter.count != 1 || !strings.Contains(counter.statements[0], "task_type IN (2, 4)") || !strings.Contains(counter.statements[0], "CASE WHEN") {
			t.Fatalf("stage read lost its bounded projection: %v", counter.statements)
		}
		for _, forbidden := range []string{"input_data", "result_data", "SELECT *", "job_instance", "FOR UPDATE"} {
			if strings.Contains(counter.statements[0], forbidden) {
				t.Fatalf("stage read loaded %s", forbidden)
			}
		}
	}
	for _, value := range []any{nil, ""} {
		if err := db.Model(&model.JobTask{}).Where("id = ?", tasks[0].ID).
			Updates(map[string]any{"status": model.TaskFailed, "result_summary": value}).Error; err != nil {
			t.Fatal(err)
		}
		facts, err := repo.FindStages(ctx, instance.ID)
		if err != nil || facts == nil || facts.Preparation == nil || (facts.Preparation.ResultSummary == nil) != (value == nil) {
			t.Fatalf("NULL/empty preparation summary collapsed: %+v err=%v", facts, err)
		}
		if value != nil && *facts.Preparation.ResultSummary != "" {
			t.Fatal("empty preparation summary was rewritten")
		}
	}
	counter.count, counter.statements = 0, nil
	post, err := repo.FindPost(ctx, instance.ID)
	if err != nil || post == nil || post.ID != tasks[1].ID || post.TaskType != model.TaskTypePost || post.Status != model.TaskFailed || post.ResultSummary != nil || counter.count != 1 {
		t.Fatalf("Post singleton projection: %+v err=%v count=%d", post, err, counter.count)
	}
	for _, forbidden := range []string{"input_data", "result_data", "result_summary", "SELECT *", "FOR UPDATE"} {
		if strings.Contains(counter.statements[0], forbidden) {
			t.Fatalf("Post lookup loaded %s", forbidden)
		}
	}
	if facts, err := repo.FindStages(ctx, instance.ID+100000); err != nil || facts == nil || facts.Preparation != nil || facts.Post != nil {
		t.Fatalf("missing stages became a query failure: %+v %v", facts, err)
	}
	if post, err := repo.FindPost(ctx, instance.ID+100000); post != nil || err != nil {
		t.Fatalf("missing Post became a record: %+v %v", post, err)
	}
}

func TestShardCountsKeepCancellationAndUnknownStatesVisible(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	ctx := context.Background()
	for _, scenario := range []struct {
		name       string
		states     []model.TaskStatus
		unfinished bool
		want       ShardCounts
	}{
		{name: "empty"},
		{name: "waiting", states: []model.TaskStatus{model.TaskWaiting}, unfinished: true, want: ShardCounts{Total: 1}},
		{name: "running", states: []model.TaskStatus{model.TaskRunning}, unfinished: true, want: ShardCounts{Total: 1}},
		{name: "retry", states: []model.TaskStatus{model.TaskRetryWaiting}, unfinished: true, want: ShardCounts{Total: 1}},
		{name: "success", states: []model.TaskStatus{model.TaskSucceeded}, want: ShardCounts{Total: 1, Succeeded: 1}},
		{name: "failure", states: []model.TaskStatus{model.TaskFailed}, want: ShardCounts{Total: 1, Failed: 1}},
		{name: "canceled", states: []model.TaskStatus{model.TaskCanceled}, want: ShardCounts{Total: 1, Canceled: 1}},
		{name: "unknown", states: []model.TaskStatus{99}, want: ShardCounts{Total: 1}},
		{name: "mixed terminal", states: []model.TaskStatus{model.TaskSucceeded, model.TaskFailed, model.TaskCanceled, 99}, want: ShardCounts{Total: 4, Succeeded: 1, Failed: 1, Canceled: 1}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, _ := createResultTask(t, db, job)
			rows := []model.JobTask{
				{InstanceID: instance.ID, TaskType: model.TaskTypePrepare, Status: model.TaskRunning},
				{InstanceID: instance.ID, TaskType: model.TaskTypePost, Status: model.TaskFailed},
			}
			for _, state := range scenario.states {
				rows = append(rows, model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeShard, Status: state,
					InputData: model.TaskData(`{"private":true}`), ResultData: model.TaskData(`{"output":true}`)})
			}
			if err := db.Create(&rows).Error; err != nil {
				t.Fatal(err)
			}
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
			if err != nil {
				t.Fatal(err)
			}
			exists, err := repo.HasUnfinishedShards(ctx, instance.ID)
			if err != nil || exists != scenario.unfinished || counter.count != 1 {
				t.Fatalf("unfinished shard existence: got=%t want=%t err=%v queries=%d", exists, scenario.unfinished, err, counter.count)
			}
			if !strings.Contains(counter.statements[0], "status IN (1, 2, 3)") || !strings.Contains(counter.statements[0], "LIMIT") {
				t.Fatalf("existence read lost its partial-index predicate or limit: %v", counter.statements)
			}
			counts, err := repo.CountShards(ctx, instance.ID)
			if err != nil || counts != scenario.want || counter.count != 2 {
				t.Fatalf("shard counts: got=%+v want=%+v err=%v queries=%d", counts, scenario.want, err, counter.count)
			}
			for _, statement := range counter.statements {
				for _, forbidden := range []string{"input_data", "result_data", "result_summary", "attempt_no", "job_instance", "FOR UPDATE", "SELECT *"} {
					if strings.Contains(statement, forbidden) {
						t.Fatalf("bounded shard facts loaded %s: %s", forbidden, statement)
					}
				}
			}
		})
	}
}

func TestPostInsertionKeepsFixedStatisticsAndOuterCommitOwnership(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	for _, counts := range []ShardCounts{{}, {Total: 3, Succeeded: 2, Failed: 1}, {Total: 3, Failed: 3}, {Total: 500, Succeeded: 500}} {
		t.Run(fmt.Sprintf("total=%d/succeeded=%d", counts.Total, counts.Succeeded), func(t *testing.T) {
			instance, _ := createRetryRepositoryTask(t, db, job, now, now)
			tx, createdAt := retryRepositoryTransaction(t, db, instance.ID)
			defer tx.Rollback()
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(tx.Session(&gorm.Session{Logger: counter}))
			if err != nil {
				t.Fatal(err)
			}
			id, err := repo.InsertPost(context.Background(), instance.ID, counts, createdAt)
			if err != nil || id <= 0 || counter.count != 1 || !strings.HasPrefix(counter.statements[0], "INSERT INTO") {
				t.Fatalf("Post creation performed more than its insert: id=%d err=%v queries=%v", id, err, counter.statements)
			}
			independent, err := NewJobTaskRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			if post, err := independent.FindPost(context.Background(), instance.ID); post != nil || err != nil {
				t.Fatalf("uncommitted Post escaped: %+v %v", post, err)
			}
			if err := tx.Commit().Error; err != nil {
				t.Fatal(err)
			}
			stored := readRepositoryTask(t, db, id)
			if stored.Status != model.TaskWaiting || stored.TaskType != model.TaskTypePost || stored.AttemptNo != 0 || stored.UsedAttempts != 0 ||
				stored.StartedAt != nil || stored.AttemptRegisteredAt != nil || stored.ExecutorInstanceID != nil || stored.ExecutorAddress != nil ||
				stored.ResultData != nil || stored.ResultSummary != nil || stored.EndReason != nil || stored.FinishedAt != nil ||
				stored.NextDispatchAt == nil || !stored.NextDispatchAt.Equal(createdAt) || !stored.CreatedAt.Equal(createdAt) || !stored.UpdatedAt.Equal(createdAt) {
				t.Fatalf("Post creation consumed execution or lost fixed times: %+v", stored)
			}
			var input map[string]int64
			if err := json.Unmarshal(stored.InputData, &input); err != nil || !reflect.DeepEqual(input, map[string]int64{
				"total": counts.Total, "succeeded": counts.Succeeded, "failed": counts.Failed,
			}) {
				t.Fatalf("fixed Post input changed fields or counts: %+v err=%v", input, err)
			}
			// A duplicated creation under the same parent protocol must fail instead
			// of returning the old Post ID or replacing its immutable statistics.
			tx, duplicateAt := retryRepositoryTransaction(t, db, instance.ID)
			defer tx.Rollback()
			repo, err = NewJobTaskRepository(tx)
			if err != nil {
				t.Fatal(err)
			}
			duplicateID, err := repo.InsertPost(context.Background(), instance.ID, ShardCounts{Total: 1, Failed: 1}, duplicateAt)
			var pgErr *pgconn.PgError
			if duplicateID != 0 || !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "uq_job_task_singleton" {
				t.Fatalf("duplicate Post was not rejected: id=%d err=%v", duplicateID, err)
			}
			if err := tx.Rollback().Error; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(stored, readRepositoryTask(t, db, id)) {
				t.Fatal("duplicate Post changed fixed input or execution facts")
			}
			tx, registeredAt := retryRepositoryTransaction(t, db, instance.ID)
			defer tx.Rollback()
			repo, err = NewJobTaskRepository(tx)
			if err != nil {
				t.Fatal(err)
			}
			registered, err := repo.RegisterAttempt(context.Background(), AttemptRegistration{
				TaskID: id, InstanceID: instance.ID, TaskType: model.TaskTypePost, ExpectedStatus: model.TaskWaiting,
				MaxAttempts: 4, RegisteredAt: registeredAt,
			})
			if err != nil || registered == nil || registered.AttemptNo != 1 || !reflect.DeepEqual(registered.InputData, stored.InputData) {
				t.Fatalf("Post first dispatch lost its fixed input: %+v %v", registered, err)
			}
			if err := tx.Commit().Error; err != nil {
				t.Fatal(err)
			}
			if applied, err := independent.SaveResult(context.Background(), TaskResultWrite{
				TaskID: id, InstanceID: instance.ID, AttemptNo: 1, ExecutorInstanceID: uuid.New(),
				Status: model.TaskSucceeded, Data: model.TaskData(`{"business":"output"}`),
			}); err != nil || !applied {
				t.Fatalf("Post result was not saved: applied=%t err=%v", applied, err)
			}
			after := readRepositoryTask(t, db, id)
			if !reflect.DeepEqual(after.InputData, stored.InputData) || after.Status != model.TaskSucceeded || after.ResultData == nil {
				t.Fatal("Post output replaced its fixed statistics")
			}
		})
	}
}

func TestPostInsertionRollbackLeavesNoRecord(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	instance, _ := createRetryRepositoryTask(t, db, job, now, now)
	tx, createdAt := retryRepositoryTransaction(t, db, instance.ID)
	defer tx.Rollback()
	repo, err := NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := repo.InsertPost(context.Background(), instance.ID, ShardCounts{}, createdAt); id <= 0 || err != nil {
		t.Fatalf("Post insertion failed before rollback: id=%d err=%v", id, err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	repo, err = NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if post, err := repo.FindPost(context.Background(), instance.ID); post != nil || err != nil {
		t.Fatalf("rolled-back Post remained: %+v %v", post, err)
	}
}

func TestStageQueriesUseExistingInstanceAndSingletonIndexes(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	if err := db.Exec(`INSERT INTO job_task (instance_id, task_type, status, attempt_no, used_attempts)
		SELECT n, 3, CASE WHEN n % 10 = 0 THEN 2 ELSE 4 END, 1, 1 FROM generate_series(1, 30000) n`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO job_task (instance_id, task_type, status, attempt_no, used_attempts)
		SELECT n, stage, 4, 1, 1 FROM generate_series(1, 30000) n CROSS JOIN (VALUES (2), (4)) AS stages(stage)`).Error; err != nil {
		t.Fatal(err)
	}
	// Give the target the largest supported shard set. Its single unfinished row
	// should use the existing partial index, while stage lookups avoid those shards.
	if err := db.Exec(`INSERT INTO job_task (instance_id, task_type, status, attempt_no, used_attempts)
		SELECT 100, 3, 4, 1, 1 FROM generate_series(1, 499)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE job_task").Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	capture := &executionQueryCapture{queryCounter: counter}
	// GORM Scan temporarily installs logger.Recorder, bypassing the session's
	// ParamsFilter. Capture the executed Row statement before GORM resets it so
	// EXPLAIN still receives the production SQL and original bound arguments.
	const captureAggregate = "test:capture_stage_aggregate"
	if err := db.Callback().Row().After("gorm:row").Register(captureAggregate, func(tx *gorm.DB) {
		capture.queries = append(capture.queries, executionQuery{
			sql: tx.Statement.SQL.String(), vars: append([]any(nil), tx.Statement.Vars...),
		})
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Callback().Row().Remove(captureAggregate); err != nil {
			t.Error(err)
		}
	})
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: capture}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, query := range []struct {
		name  string
		index string
		run   func() error
	}{
		{"stages", "uq_job_task_singleton", func() error { _, err := repo.FindStages(ctx, 100); return err }},
		{"Post", "uq_job_task_singleton", func() error { _, err := repo.FindPost(ctx, 100); return err }},
		{"unfinished", "idx_job_task_unfinished_instance", func() error { _, err := repo.HasUnfinishedShards(ctx, 100); return err }},
		{"counts", "idx_job_task_instance", func() error { _, err := repo.CountShards(ctx, 100); return err }},
	} {
		t.Run(query.name, func(t *testing.T) {
			capture.queries, counter.count, counter.statements = nil, 0, nil
			if err := query.run(); err != nil || counter.count != 1 || len(capture.queries) != 1 {
				t.Fatalf("stage query is not one statement: err=%v count=%d", err, counter.count)
			}
			plan := explainExecutionQuery(t, db, capture.queries[0])
			if !strings.Contains(plan, query.index) || strings.Contains(plan, "Seq Scan") {
				t.Fatalf("stage query lost its existing indexed access path:\n%s", plan)
			}
		})
	}
}
