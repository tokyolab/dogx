//go:build integration

package repository

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestPreparationQueriesLoadOnlyRequestedFacts(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, task := createResultTask(t, db, job)
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(map[string]any{
		"task_type": model.TaskTypePrepare, "status": model.TaskSucceeded,
		"result_data": model.TaskData(`[null,9007199254740993,{"orders":[1,2]}]`),
	}).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pre, err := repo.FindPreparation(ctx, instance.ID)
	if err != nil || pre == nil || pre.ID != task.ID || pre.Status != model.TaskSucceeded || counter.count != 1 {
		t.Fatalf("preparation projection: %+v err=%v count=%d", pre, err, counter.count)
	}
	for _, forbidden := range []string{"input_data", "result_data", "result_summary", "SELECT *", "FOR UPDATE"} {
		if strings.Contains(counter.statements[0], forbidden) {
			t.Fatalf("minimal preparation read loaded %s", forbidden)
		}
	}
	full, err := repo.FindPreparationResult(ctx, instance.ID)
	stored := readRepositoryTask(t, db, task.ID)
	if err != nil || full == nil || full.ID != pre.ID || full.Status != pre.Status || !reflect.DeepEqual(full.Data, stored.ResultData) {
		t.Fatalf("preparation output projection: %+v err=%v", full, err)
	}
	if exists, err := repo.HasShards(ctx, instance.ID); exists || err != nil {
		t.Fatalf("preparation counted as a shard: exists=%t err=%v", exists, err)
	}
	if missing, err := repo.FindPreparation(ctx, instance.ID+100000); missing != nil || err != nil {
		t.Fatalf("missing preparation: %+v %v", missing, err)
	}
	if missing, err := repo.FindPreparationResult(ctx, instance.ID+100000); missing != nil || err != nil {
		t.Fatalf("missing preparation result: %+v %v", missing, err)
	}
}

func TestShardInsertionIsOneCompleteStatementOwnedByParentTransaction(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	for _, scenario := range []struct {
		name   string
		commit bool
		bad    bool
	}{{"commit", true, false}, {"rollback", false, false}, {"invalid final item", false, true}} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, _ := createRetryRepositoryTask(t, db, job, now, now)
			tx, generatedAt := retryRepositoryTransaction(t, db, instance.ID)
			defer tx.Rollback()
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobTaskRepository(tx.Session(&gorm.Session{Logger: counter, CreateBatchSize: 1}))
			if err != nil {
				t.Fatal(err)
			}
			items := make([]model.TaskData, 500)
			for i := range items {
				items[i] = model.TaskData(`9007199254740993`)
			}
			items[0], items[1] = model.TaskData(`null`), model.TaskData(`"business input"`)
			if scenario.bad {
				items[len(items)-1] = model.TaskData(`{"invalid":"\u0000"}`)
			}
			err = repo.InsertShards(context.Background(), instance.ID, items, generatedAt)
			if scenario.bad {
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "22P05" {
					t.Fatalf("invalid last item did not fail the complete statement: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if counter.count != 1 || !strings.HasPrefix(counter.statements[0], "INSERT INTO") {
				t.Fatalf("generation split the complete list: %v", counter.statements)
			}
			var visible int64
			if err := db.Model(&model.JobTask{}).Where("instance_id = ? AND task_type = ?", instance.ID, model.TaskTypeShard).Count(&visible).Error; err != nil || visible != 0 {
				t.Fatalf("outer transaction was committed by insertion: count=%d err=%v", visible, err)
			}
			if scenario.commit {
				err = tx.Commit().Error
			} else {
				err = tx.Rollback().Error
			}
			if err != nil {
				t.Fatal(err)
			}
			var shards []model.JobTask
			if err := db.Where("instance_id = ? AND task_type = ?", instance.ID, model.TaskTypeShard).Order("id").Find(&shards).Error; err != nil {
				t.Fatal(err)
			}
			if !scenario.commit {
				if len(shards) != 0 {
					t.Fatalf("failed generation retained %d shards", len(shards))
				}
				return
			}
			if len(shards) != len(items) {
				t.Fatalf("complete-list size changed: got=%d want=%d", len(shards), len(items))
			}
			for i, shard := range shards {
				if shard.Status != model.TaskWaiting || shard.AttemptNo != 0 || shard.UsedAttempts != 0 ||
					shard.StartedAt != nil || shard.AttemptRegisteredAt != nil || shard.NextDispatchAt == nil ||
					!shard.NextDispatchAt.Equal(generatedAt) || !shard.CreatedAt.Equal(generatedAt) || !shard.UpdatedAt.Equal(generatedAt) ||
					shard.ResultData != nil || !reflect.DeepEqual(shard.InputData, items[i]) {
					t.Fatalf("generated shard %d lost initial facts: %+v", i, shard)
				}
			}
		})
	}
}

func TestParentLockPreventsDuplicateShardSet(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	instance, _ := createRetryRepositoryTask(t, db, job, now, now)
	first, createdAt := retryRepositoryTransaction(t, db, instance.ID)
	defer first.Rollback()
	firstTasks, err := NewJobTaskRepository(first)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := firstTasks.HasShards(context.Background(), instance.ID); exists || err != nil {
		t.Fatalf("new instance already has shards: %t %v", exists, err)
	}
	if err := firstTasks.InsertShards(context.Background(), instance.ID, []model.TaskData{model.TaskData(`1`), model.TaskData(`1`)}, createdAt); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	second := db.WithContext(ctx).Begin()
	defer second.Rollback()
	parents, err := NewJobInstanceRepository(second)
	if err != nil {
		t.Fatal(err)
	}
	if parent, err := parents.LockForExecution(ctx, instance.ID); parent != nil || !errors.Is(err, ErrInstanceLocked) {
		t.Fatalf("competing generator entered the same parent: %+v %v", parent, err)
	}
	if err := second.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if err := first.Commit().Error; err != nil {
		t.Fatal(err)
	}
	third, _ := retryRepositoryTransaction(t, db, instance.ID)
	defer third.Rollback()
	tasks, err := NewJobTaskRepository(third)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := tasks.HasShards(ctx, instance.ID); !exists || err != nil {
		t.Fatalf("later generator failed to see committed set: %t %v", exists, err)
	}
	var count int64
	if err := third.Model(&model.JobTask{}).Where("instance_id = ? AND task_type = ?", instance.ID, model.TaskTypeShard).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("duplicate business values were deduplicated or set duplicated: count=%d err=%v", count, err)
	}
}

func TestDispatchCandidatesCombineFirstStagesAndRetries(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	var expected []DispatchCandidate
	for _, stage := range []model.TaskType{model.TaskTypeShard, model.TaskTypePost, model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
		instance, task := createRetryRepositoryTask(t, db, job, now, now)
		fields := map[string]any{"task_type": stage}
		state, attempt := model.TaskRetryWaiting, int64(1)
		if len(expected) < 2 {
			state, attempt = model.TaskWaiting, 0
			fields["status"], fields["attempt_no"], fields["used_attempts"] = state, attempt, 0
			fields["started_at"], fields["attempt_registered_at"] = nil, nil
		}
		if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(fields).Error; err != nil {
			t.Fatal(err)
		}
		expected = append(expected, DispatchCandidate{TaskID: task.ID, InstanceID: instance.ID, TaskType: stage, Status: state, AttemptNo: attempt, NextDispatchAt: now})
	}
	for name, invalid := range map[string]map[string]any{
		"ordinary waiting": {"task_type": model.TaskTypeOrdinary},
		"prepare waiting":  {"task_type": model.TaskTypePrepare},
		"attempt":          {"attempt_no": 1},
		"budget":           {"used_attempts": 1},
		"started":          {"started_at": now},
		"registered":       {"attempt_registered_at": now},
	} {
		t.Run(name, func(t *testing.T) {
			_, task := createRetryRepositoryTask(t, db, job, now, now)
			fields := map[string]any{"task_type": model.TaskTypeShard, "status": model.TaskWaiting,
				"attempt_no": 0, "used_attempts": 0, "started_at": nil, "attempt_registered_at": nil}
			for key, value := range invalid {
				fields[key] = value
			}
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(fields).Error; err != nil {
				t.Fatal(err)
			}
		})
	}
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	var all []DispatchCandidate
	var cursor *DispatchCursor
	for {
		page, err := repo.ListDispatchCandidates(context.Background(), now, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		last := page[len(page)-1]
		cursor = &DispatchCursor{TaskID: last.TaskID, NextDispatchAt: last.NextDispatchAt}
	}
	if !reflect.DeepEqual(all, expected) {
		t.Fatalf("unified pages changed original identity: got=%+v want=%+v", all, expected)
	}
	for _, candidate := range expected {
		page, err := repo.ListInstanceDispatchCandidates(context.Background(), candidate.InstanceID, now, 128)
		if err != nil || !reflect.DeepEqual(page, []DispatchCandidate{candidate}) {
			t.Fatalf("targeted hint changed candidate rules: %+v %v", page, err)
		}
	}
}

func TestFirstStageRegistrationRetainsInputAcrossRetry(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	for _, stage := range []model.TaskType{model.TaskTypeShard, model.TaskTypePost} {
		t.Run(fmt.Sprint(stage), func(t *testing.T) {
			instance, task := createRetryRepositoryTask(t, db, job, now, now)
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(map[string]any{
				"task_type": stage, "status": model.TaskWaiting, "attempt_no": 0, "used_attempts": 0,
				"started_at": nil, "attempt_registered_at": nil, "input_data": model.TaskData(`{"id":9007199254740993}`),
			}).Error; err != nil {
				t.Fatal(err)
			}
			before := readRepositoryTask(t, db, task.ID)
			tx, registeredAt := retryRepositoryTransaction(t, db, instance.ID)
			defer tx.Rollback()
			repo, err := NewJobTaskRepository(tx)
			if err != nil {
				t.Fatal(err)
			}
			write := AttemptRegistration{TaskID: task.ID, InstanceID: instance.ID, TaskType: stage,
				ExpectedStatus: model.TaskWaiting, MaxAttempts: 4, RegisteredAt: registeredAt}
			registered, err := repo.RegisterAttempt(context.Background(), write)
			if err != nil || registered == nil || registered.TaskType != stage || registered.AttemptNo != 1 || registered.UsedAttempts != 1 || !reflect.DeepEqual(registered.InputData, before.InputData) {
				t.Fatalf("first stage registration: %+v %v", registered, err)
			}
			if !reflect.DeepEqual(before, readRepositoryTask(t, db, task.ID)) {
				t.Fatal("registration committed the outer transaction")
			}
			if err := tx.Commit().Error; err != nil {
				t.Fatal(err)
			}
			first := readRepositoryTask(t, db, task.ID)
			if first.StartedAt == nil || !first.StartedAt.Equal(registeredAt) || first.AttemptRegisteredAt == nil || !first.AttemptRegisteredAt.Equal(registeredAt) {
				t.Fatalf("first registration did not set both start times: %+v", first)
			}
			independent, err := NewJobTaskRepository(db)
			if err != nil {
				t.Fatal(err)
			}
			if applied, err := independent.SaveDispatchBusy(context.Background(), DispatchBusyWrite{TaskID: task.ID, InstanceID: instance.ID, AttemptNo: 1, Delay: time.Microsecond}); err != nil || !applied {
				t.Fatalf("stage BUSY was not saved: %t %v", applied, err)
			}
			tx, registeredAt = retryRepositoryTransaction(t, db, instance.ID)
			defer tx.Rollback()
			repo, err = NewJobTaskRepository(tx)
			if err != nil {
				t.Fatal(err)
			}
			write.RegisteredAt = registeredAt
			if stale, err := repo.RegisterAttempt(context.Background(), write); stale != nil || err != nil {
				t.Fatalf("stale first-stage candidate changed retry: %+v %v", stale, err)
			}
			write.ExpectedStatus, write.ExpectedAttemptNo = model.TaskRetryWaiting, 1
			registered, err = repo.RegisterAttempt(context.Background(), write)
			if err != nil || registered == nil || registered.AttemptNo != 2 || registered.UsedAttempts != 1 || !reflect.DeepEqual(registered.InputData, before.InputData) {
				t.Fatalf("retry lost stage/input/refund: %+v %v", registered, err)
			}
			if err := tx.Commit().Error; err != nil {
				t.Fatal(err)
			}
			after := readRepositoryTask(t, db, task.ID)
			if !after.StartedAt.Equal(*first.StartedAt) || !reflect.DeepEqual(after.InputData, before.InputData) {
				t.Fatal("retry changed first-start or fixed input")
			}
		})
	}
}

func TestFirstStageRegistrationRejectsPartiallyRegisteredRows(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	for name, invalid := range map[string]map[string]any{
		"consumed opportunity": {"used_attempts": 1},
		"first start":          {"started_at": now},
		"registration time":    {"attempt_registered_at": now},
		"changed attempt":      {"attempt_no": 1},
		"changed stage":        {"task_type": model.TaskTypePost},
		"changed state":        {"status": model.TaskRetryWaiting},
		"not due":              {"next_dispatch_at": now.Add(time.Hour)},
	} {
		t.Run(name, func(t *testing.T) {
			instance, task := createRetryRepositoryTask(t, db, job, now, now)
			fields := map[string]any{"task_type": model.TaskTypeShard, "status": model.TaskWaiting,
				"attempt_no": 0, "used_attempts": 0, "started_at": nil, "attempt_registered_at": nil,
				"input_data": model.TaskData(`null`)}
			for key, value := range invalid {
				fields[key] = value
			}
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(fields).Error; err != nil {
				t.Fatal(err)
			}
			before := readRepositoryTask(t, db, task.ID)
			tx, registeredAt := retryRepositoryTransaction(t, db, instance.ID)
			defer tx.Rollback()
			repo, err := NewJobTaskRepository(tx)
			if err != nil {
				t.Fatal(err)
			}
			registered, err := repo.RegisterAttempt(context.Background(), AttemptRegistration{
				TaskID: task.ID, InstanceID: instance.ID, TaskType: model.TaskTypeShard,
				ExpectedStatus: model.TaskWaiting, MaxAttempts: 4, RegisteredAt: registeredAt,
			})
			if registered != nil || err != nil {
				t.Fatalf("ineligible first attempt: %+v %v", registered, err)
			}
			if err := tx.Commit().Error; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, readRepositoryTask(t, db, task.ID)) {
				t.Fatal("rejected first registration changed execution facts")
			}
		})
	}
}

func TestDispatchFailureWritesSupportEveryStageWithoutChangingInput(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	for _, stage := range []model.TaskType{model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost} {
		for _, operation := range []string{"busy", "rejection", "unknown"} {
			t.Run(fmt.Sprintf("stage=%d/%s", stage, operation), func(t *testing.T) {
				instance, task := createUnknownRepositoryTask(t, db, job, now)
				fields := map[string]any{"task_type": stage}
				if stage == model.TaskTypeShard || stage == model.TaskTypePost {
					fields["input_data"] = model.TaskData(`{"fixed":true}`)
				}
				if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Updates(fields).Error; err != nil {
					t.Fatal(err)
				}
				before := readRepositoryTask(t, db, task.ID)
				repo, err := NewJobTaskRepository(db)
				if err != nil {
					t.Fatal(err)
				}
				applied := false
				switch operation {
				case "busy":
					applied, err = repo.SaveDispatchBusy(context.Background(), DispatchBusyWrite{
						TaskID: task.ID, InstanceID: instance.ID, AttemptNo: before.AttemptNo, Delay: time.Second,
					})
				case "rejection":
					applied, err = repo.SaveDispatchRejection(context.Background(), DispatchRejectionWrite{
						TaskID: task.ID, InstanceID: instance.ID, AttemptNo: before.AttemptNo, MaxRetries: 3,
						RetryIntervalSeconds: 10, Summary: "executor rejected task before admission",
					})
				case "unknown":
					var settled *SettledTask
					settled, err = repo.SettleUnknown(context.Background(), UnknownSettlement{
						TaskID: task.ID, InstanceID: instance.ID, ExpectedAttemptNo: before.AttemptNo, MaxAttempts: 4,
						ProcessedAt: now, ObservationCutoff: now.Add(-30 * time.Second), RetryAt: now.Add(10 * time.Second),
					})
					applied = settled != nil
				}
				if !applied || err != nil {
					t.Fatalf("stage response was not persisted: applied=%t err=%v", applied, err)
				}
				after := readRepositoryTask(t, db, task.ID)
				wantUsed := before.UsedAttempts
				if operation == "busy" {
					wantUsed--
				}
				if after.Status != model.TaskRetryWaiting || after.TaskType != stage || after.AttemptNo != before.AttemptNo ||
					after.UsedAttempts != wantUsed || !reflect.DeepEqual(after.InputData, before.InputData) ||
					!reflect.DeepEqual(after.StartedAt, before.StartedAt) || !reflect.DeepEqual(after.AttemptRegisteredAt, before.AttemptRegisteredAt) {
					t.Fatalf("response changed stage/input/identity: before=%+v after=%+v", before, after)
				}
			})
		}
	}
}

func TestInitialPreparationUsesParentStartTimeAndSingletonConstraint(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	now := retryRepositoryTime(t, db)
	instance, _ := createRetryRepositoryTask(t, db, job, now, now)
	tx, startedAt := retryRepositoryTransaction(t, db, instance.ID)
	defer tx.Rollback()
	repo, err := NewJobTaskRepository(tx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := repo.InsertInitial(context.Background(), instance.ID, model.TaskTypePrepare, startedAt)
	if err != nil || id <= 0 {
		t.Fatalf("preparation creation failed: id=%d err=%v", id, err)
	}
	pre := readRepositoryTask(t, tx, id)
	if pre.TaskType != model.TaskTypePrepare || pre.Status != model.TaskRunning || pre.AttemptNo != 1 || pre.UsedAttempts != 1 ||
		pre.InputData != nil || pre.ResultData != nil || pre.NextDispatchAt != nil || pre.StartedAt == nil || pre.AttemptRegisteredAt == nil ||
		!pre.StartedAt.Equal(startedAt) || !pre.AttemptRegisteredAt.Equal(startedAt) || !pre.CreatedAt.Equal(startedAt) || !pre.UpdatedAt.Equal(startedAt) {
		t.Fatalf("initial preparation facts differ from parent start: %+v", pre)
	}
	_, err = repo.InsertInitial(context.Background(), instance.ID, model.TaskTypePrepare, startedAt)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "uq_job_task_singleton" {
		t.Fatalf("duplicate preparation did not reject outer transaction: %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	independent, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	if item, err := independent.FindPreparation(context.Background(), instance.ID); item != nil || err != nil {
		t.Fatalf("rolled-back preparation remained: %+v %v", item, err)
	}
}
