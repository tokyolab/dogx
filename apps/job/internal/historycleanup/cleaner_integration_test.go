//go:build integration

package historycleanup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
)

func TestCleanupContinuesAfterInconsistentHistoryWithoutPartialDeletion(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	now, err := instances.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-8 * 24 * time.Hour)
	broken := cleanupParent(t, db, model.InstanceSucceeded, old)
	valid := cleanupParent(t, db, model.InstanceSucceeded, old)
	children := []model.JobTask{
		{InstanceID: broken.ID, TaskType: model.TaskTypePrepare, Status: model.TaskSucceeded, FinishedAt: &old},
		{InstanceID: broken.ID, TaskType: model.TaskTypeShard, Status: model.TaskRunning},
		{InstanceID: valid.ID, TaskType: model.TaskTypeOrdinary, Status: model.TaskSucceeded, FinishedAt: &old},
	}
	for i := range children {
		children[i].CreatedAt, children[i].UpdatedAt = old, old
	}
	if err := db.Create(&children).Error; err != nil {
		t.Fatal(err)
	}
	runner, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(instances, runner, Config{Retention: 7 * 24 * time.Hour, Interval: 4 * time.Hour,
		PageSize: 1, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.scan(context.Background())
	if !errors.Is(err, errInvalidHistory) || result.failed != 1 || result.failedInstance != broken.ID || result.deleted != 1 || result.tasks != 1 {
		t.Fatalf("scan=%+v error=%v", result, err)
	}
	var parents, tasks int64
	if err := db.Model(&model.JobInstance{}).Where("id = ?", broken.ID).Count(&parents).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.JobTask{}).Where("instance_id = ?", broken.ID).Count(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	if parents != 1 || tasks != 2 {
		t.Fatalf("inconsistent history was partially deleted: parents=%d tasks=%d", parents, tasks)
	}
	if err := db.Model(&model.JobInstance{}).Where("id = ?", valid.ID).Count(&parents).Error; err != nil || parents != 0 {
		t.Fatalf("bad history blocked a later candidate: parents=%d error=%v", parents, err)
	}
}

func TestCleanupRealTraversalWithFullShardHistoryAndActiveTaskLock(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	now, err := instances.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-8 * 24 * time.Hour)
	var expiredIDs []int64
	for i := 0; i < 105; i++ {
		parent := cleanupParent(t, db, model.InstanceSucceeded, old)
		expiredIDs = append(expiredIDs, parent.ID)
		task := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeOrdinary, Status: model.TaskSucceeded,
			AttemptNo: 1, UsedAttempts: 1, FinishedAt: &old, CreatedAt: old, UpdatedAt: old}
		if err := db.Create(&task).Error; err != nil {
			t.Fatal(err)
		}
	}
	sharded := cleanupParent(t, db, model.InstanceSucceeded, old)
	sharded.ExecutionConfig.ExecutionMode = model.ExecutionModeSharded
	sharded.ExecutionConfig.PostEnabled = true
	if err := db.Model(&sharded).Update("execution_config", sharded.ExecutionConfig).Error; err != nil {
		t.Fatal(err)
	}
	expiredIDs = append(expiredIDs, sharded.ID)
	// Deterministic, poorly compressible JSON exercises real TOAST deletion.
	// This is a bounded data-shape regression, not a production throughput claim.
	var text strings.Builder
	text.WriteByte('"')
	for i := 0; i < 256; i++ {
		hash := sha256.Sum256([]byte(strconv.Itoa(i)))
		text.WriteString(hex.EncodeToString(hash[:]))
	}
	text.WriteByte('"')
	payload := model.TaskData(text.String())
	children := make([]model.JobTask, 502)
	for i := range children {
		kind := model.TaskTypeShard
		if i == 0 {
			kind = model.TaskTypePrepare
		} else if i == len(children)-1 {
			kind = model.TaskTypePost
		}
		children[i] = model.JobTask{InstanceID: sharded.ID, TaskType: kind, Status: model.TaskSucceeded,
			AttemptNo: 1, UsedAttempts: 1, FinishedAt: &old, CreatedAt: old, UpdatedAt: old,
			InputData: payload, ResultData: payload}
	}
	if err := db.CreateInBatches(&children, 32).Error; err != nil {
		t.Fatal(err)
	}
	recent := cleanupParent(t, db, model.InstanceSucceeded, now)
	active := cleanupParent(t, db, model.InstanceRunning, now)
	activeTask := model.JobTask{InstanceID: active.ID, TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning,
		AttemptNo: 1, UsedAttempts: 1, CreatedAt: now, UpdatedAt: now}
	if err := db.Create(&activeTask).Error; err != nil {
		t.Fatal(err)
	}
	locked := db.Begin()
	if locked.Error != nil {
		t.Fatal(locked.Error)
	}
	defer func() { _ = locked.Rollback().Error }()
	if err := locked.Model(&model.JobTask{}).Where("id = ?", activeTask.ID).Update("updated_at", now).Error; err != nil {
		t.Fatal(err)
	}
	runner, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(instances, runner, Config{Retention: 7 * 24 * time.Hour, Interval: 4 * time.Hour,
		PageSize: 100, OperationTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	started := time.Now()
	result, err := c.scan(ctx)
	if err != nil || result.deleted != 106 || result.tasks != 607 || result.skipped != 0 {
		t.Fatalf("scan=%+v error=%v", result, err)
	}
	t.Logf("cleaned %d Instances and %d Tasks in %s; 502-Task round contained %d JSON bytes per Task", result.deleted, result.tasks, time.Since(started), len(payload)*2)
	for _, record := range []struct {
		model any
		where string
	}{
		{&model.JobInstance{}, "id IN ?"},
		{&model.JobTask{}, "instance_id IN ?"},
	} {
		var count int64
		if err := db.Model(record.model).Where(record.where, expiredIDs).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("expired records remain: count=%d error=%v", count, err)
		}
	}
	var retained int64
	if err := db.Model(&model.JobInstance{}).Where("id IN ?", []int64{active.ID, recent.ID}).Count(&retained).Error; err != nil || retained != 2 {
		t.Fatalf("active or recently finished parent deleted: count=%d error=%v", retained, err)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ? AND status = ?", activeTask.ID, model.TaskRunning).Count(&retained).Error; err != nil || retained != 1 {
		t.Fatalf("cleanup touched the locked active Task: count=%d error=%v", retained, err)
	}
}

func cleanupParent(t *testing.T, db *gorm.DB, status model.InstanceStatus, finished time.Time) model.JobInstance {
	t.Helper()
	requestID, operator := uuid.New(), int64(1)
	parent := model.JobInstance{JobID: 1, JobName: "cleanup fixture", ExecutorGroupID: 1,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operator,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ExecutionMode: model.ExecutionModeNormal,
			ProcessorName: "test.cleanup", RetryIntervalSeconds: 10, TimeoutSeconds: 900},
		Status: status, CreatedAt: finished.Add(-time.Hour), UpdatedAt: finished}
	if status == model.InstanceRunning {
		parent.StartedAt = &finished
		deadline := finished.Add(15 * time.Minute)
		parent.DeadlineAt = &deadline
	} else {
		parent.FinishedAt = &finished
	}
	if err := db.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	return parent
}
