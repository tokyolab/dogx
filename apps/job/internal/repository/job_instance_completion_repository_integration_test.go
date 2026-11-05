//go:build integration

package repository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestInstanceCompletionReadsParentAndNullableChildInOneSnapshot(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, task := createResultTask(t, db, job)
	// A scalar JSONB snapshot cannot decode into ExecutionConfig. Terminal-parent
	// checks must still read it successfully and decide whether decoding is needed.
	raw := `"unusable frozen configuration"`
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
		Updates(map[string]any{"execution_config": gorm.Expr("?::jsonb", raw), "deadline_at": nil}).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	read := func() *InstanceCompletion {
		t.Helper()
		counter.count, counter.statements = 0, nil
		item, err := repo.FindCompletion(context.Background(), instance.ID)
		if err != nil || item == nil || item.ID != instance.ID || string(item.ExecutionConfig) != raw || item.DeadlineAt != nil {
			t.Fatalf("unexpected completion projection: item=%+v err=%v", item, err)
		}
		if counter.count != 1 || !strings.Contains(counter.statements[0], "LEFT JOIN job_task AS t ON t.instance_id = i.id AND t.task_type = 1") {
			t.Fatalf("completion did not read one parent/child snapshot: %v", counter.statements)
		}
		for _, forbidden := range []string{"result_data", "job_definition", "FOR UPDATE", "SELECT *"} {
			if strings.Contains(counter.statements[0], forbidden) {
				t.Fatalf("completion added %s: %s", forbidden, counter.statements[0])
			}
		}
		return item
	}
	item := read()
	if item.Status != model.InstanceRunning || item.TaskID == nil || *item.TaskID != task.ID ||
		item.TaskStatus == nil || *item.TaskStatus != model.TaskRunning || item.ResultSummary != nil {
		t.Fatalf("nullable running Task changed: %+v", item)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).
		Updates(map[string]any{"status": model.TaskSucceeded, "result_summary": ""}).Error; err != nil {
		t.Fatal(err)
	}
	item = read()
	if item.TaskStatus == nil || *item.TaskStatus != model.TaskSucceeded || item.ResultSummary == nil || *item.ResultSummary != "" {
		t.Fatalf("empty successful summary became absent: %+v", item)
	}
	if err := db.Delete(&model.JobTask{}, task.ID).Error; err != nil {
		t.Fatal(err)
	}
	// A different stage must not masquerade as the ordinary singleton, and the
	// JOIN condition must not remove a parent whose ordinary Task is missing.
	if err := db.Create(&model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeShard, Status: model.TaskSucceeded}).Error; err != nil {
		t.Fatal(err)
	}
	for _, state := range []model.InstanceStatus{model.InstanceRunning, model.InstanceWaiting, model.InstanceStopping, model.InstanceSucceeded, 99} {
		if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
		item = read()
		if item.Status != state || item.TaskID != nil || item.TaskStatus != nil || item.ResultSummary != nil {
			t.Fatalf("missing ordinary child or parent status was changed: %+v", item)
		}
	}
	if missing, err := repo.FindCompletion(context.Background(), instance.ID+1000); missing != nil || err != nil {
		t.Fatalf("absent parent: item=%+v err=%v", missing, err)
	}
}

func TestInstanceManagementReadSurvivesJobAndGroupDeletion(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, _ := createResultTask(t, db, job)
	before := readCompletionInstance(t, db, instance.ID)
	if err := db.Model(&model.JobDefinition{}).Where("id = ?", job.ID).
		Updates(map[string]any{"params": "edited later", "processor_name": "new.processor"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(job).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("id = ?", job.ExecutorGroupID).Delete(&model.ExecutorGroup{}).Error; err != nil {
		t.Fatal(err)
	}
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindByID(context.Background(), instance.ID)
	if err != nil || got == nil || !reflect.DeepEqual(before, *got) {
		t.Fatalf("management read lost persisted facts: got=%+v err=%v", got, err)
	}
	if missing, err := repo.FindByID(context.Background(), instance.ID+1000); missing != nil || !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("absent management record: item=%+v err=%v", missing, err)
	}
}

func TestInstanceExistsIgnoresFrozenConfigurationAndKeepsDatabaseFailures(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, _ := createResultTask(t, db, job)
	if err := db.Delete(job).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("id = ?", job.ExecutorGroupID).Delete(&model.ExecutorGroup{}).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []string{`null`, `"unusable snapshot"`, `{"schema_version":"invalid","execution_mode":{}}`} {
		if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).
			Update("execution_config", gorm.Expr("?::jsonb", snapshot)).Error; err != nil {
			t.Fatal(err)
		}
		counter.count, counter.statements = 0, nil
		if exists, err := repo.Exists(context.Background(), instance.ID); !exists || err != nil {
			t.Fatalf("persisted parent with unusable snapshot became absent: exists=%v err=%v", exists, err)
		}
		if counter.count != 1 || len(counter.statements) != 1 || !strings.HasPrefix(counter.statements[0], "SELECT EXISTS (") {
			t.Fatalf("existence did not use one bounded query: %v", counter.statements)
		}
		for _, forbidden := range []string{"execution_config", "job_definition", "job_executor_group", "JOIN", "FOR UPDATE", "SELECT *"} {
			if strings.Contains(counter.statements[0], forbidden) {
				t.Fatalf("existence query unnecessarily accessed %s: %s", forbidden, counter.statements[0])
			}
		}
	}
	if exists, err := repo.Exists(context.Background(), instance.ID+1000); exists || err != nil {
		t.Fatalf("missing parent existence: exists=%v err=%v", exists, err)
	}
	if err := db.Exec("ALTER TABLE job_instance RENAME TO unavailable_instance").Error; err != nil {
		t.Fatal(err)
	}
	exists, err := repo.Exists(context.Background(), instance.ID)
	var driverErr *pgconn.PgError
	if exists || !errors.As(err, &driverErr) || driverErr.Code != "42P01" || errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("database failure became absence: exists=%v err=%v", exists, err)
	}
}

func TestInstanceCompletionPersistsTerminalFactsAfterDeadline(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	empty, summary := "", "executor refused the final attempt"
	for _, scenario := range []struct {
		name    string
		target  model.InstanceStatus
		summary *string
	}{
		{"success without summary", model.InstanceSucceeded, nil},
		{"success with empty summary", model.InstanceSucceeded, &empty},
		{"failure summary", model.InstanceFailed, &summary},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			instance, task := createResultTask(t, db, job)
			taskStatus, taskReason := model.TaskSucceeded, model.EndReasonSuccess
			if scenario.target == model.InstanceFailed {
				taskStatus, taskReason = model.TaskFailed, model.EndReasonDispatchRejected
			}
			if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).
				Updates(map[string]any{"status": taskStatus, "end_reason": taskReason, "result_summary": scenario.summary}).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("result_summary", "previous summary").Error; err != nil {
				t.Fatal(err)
			}
			before, childBefore := readCompletionInstance(t, db, instance.ID), readRepositoryTask(t, db, task.ID)
			counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
			repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: counter, SkipDefaultTransaction: true}))
			if err != nil {
				t.Fatal(err)
			}
			var databaseBefore time.Time
			if err := db.Raw("SELECT clock_timestamp()").Scan(&databaseBefore).Error; err != nil {
				t.Fatal(err)
			}
			if changed, err := repo.Complete(context.Background(), instance.ID, scenario.target, scenario.summary); !changed || err != nil {
				t.Fatalf("complete expired Instance: changed=%v err=%v", changed, err)
			}
			if counter.count != 1 || !strings.HasPrefix(counter.statements[0], "UPDATE ") ||
				strings.Contains(counter.statements[0], "deadline_at") || strings.Contains(counter.statements[0], " FROM ") {
				t.Fatalf("completion exceeded its single conditional update: %v", counter.statements)
			}
			after := readCompletionInstance(t, db, instance.ID)
			wantReason := model.EndReasonSuccess
			if scenario.target == model.InstanceFailed {
				wantReason = model.EndReasonExecutionFailed
			}
			if after.Status != scenario.target || after.EndReason == nil || *after.EndReason != wantReason ||
				!reflect.DeepEqual(after.ResultSummary, scenario.summary) || after.FinishedAt == nil ||
				after.FinishedAt.Before(databaseBefore) || after.UpdatedAt.Before(databaseBefore) {
				t.Fatalf("incorrect completion facts: %+v", after)
			}
			before.Status, before.EndReason, before.ResultSummary = after.Status, after.EndReason, after.ResultSummary
			before.FinishedAt, before.UpdatedAt = after.FinishedAt, after.UpdatedAt
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(childBefore, readRepositoryTask(t, db, task.ID)) {
				t.Fatal("completion changed frozen parent or child execution facts")
			}
			counter.count, counter.statements = 0, nil
			if changed, err := repo.Complete(context.Background(), instance.ID, model.InstanceFailed, nil); changed || err != nil || counter.count != 1 {
				t.Fatalf("duplicate completion: changed=%v err=%v queries=%d", changed, err, counter.count)
			}
			if !reflect.DeepEqual(after, readCompletionInstance(t, db, instance.ID)) {
				t.Fatal("duplicate completion overwrote a final outcome")
			}
		})
	}
}

func TestInstanceCompletionDoesNotChangeIneligibleParents(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceStopping, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		instance, _ := createResultTask(t, db, job)
		if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
		before := readCompletionInstance(t, db, instance.ID)
		if changed, err := repo.Complete(context.Background(), instance.ID, model.InstanceSucceeded, nil); changed || err != nil {
			t.Fatalf("ineligible status %d: changed=%v err=%v", state, changed, err)
		}
		if !reflect.DeepEqual(before, readCompletionInstance(t, db, instance.ID)) {
			t.Fatalf("completion overwrote status %d", state)
		}
	}
	if changed, err := repo.Complete(context.Background(), 999999, model.InstanceSucceeded, nil); changed || err != nil {
		t.Fatalf("missing parent completion: changed=%v err=%v", changed, err)
	}
}

func TestInstanceCompletionDatabaseFailureDoesNotClaimSuccess(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, _ := createResultTask(t, db, job)
	before := readCompletionInstance(t, db, instance.ID)
	repo, err := NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	invalid := "invalid\x00text"
	changed, err := repo.Complete(context.Background(), instance.ID, model.InstanceSucceeded, &invalid)
	var driverErr *pgconn.PgError
	if changed || !errors.As(err, &driverErr) {
		t.Fatalf("invalid PostgreSQL text appeared saved: changed=%v err=%v", changed, err)
	}
	if !reflect.DeepEqual(before, readCompletionInstance(t, db, instance.ID)) {
		t.Fatal("failed completion partially changed its parent")
	}
	if changed, err := repo.Complete(context.Background(), instance.ID, model.InstanceSucceeded, nil); !changed || err != nil {
		t.Fatalf("write after failed transaction: changed=%v err=%v", changed, err)
	}
}

func readCompletionInstance(t *testing.T, db *gorm.DB, id int64) model.JobInstance {
	t.Helper()
	var instance model.JobInstance
	if err := db.Where("id = ?", id).Take(&instance).Error; err != nil {
		t.Fatal(err)
	}
	return instance
}
