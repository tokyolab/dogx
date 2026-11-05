//go:build integration

package svc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
)

func TestServiceRecoversUnboundAttemptsWithoutLeadership(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	now, err := instances.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	type fixture struct {
		parent model.JobInstance
		task   model.JobTask
		want   model.TaskStatus
	}
	var fixtures []fixture
	for _, kind := range []string{"exhausted", "exhausted", "exhausted", "retry", "recent", "bound", "expired"} {
		started, deadline, registered := now.Add(-time.Minute), now.Add(4*time.Minute), now.Add(-time.Minute)
		if kind == "recent" {
			registered = now
		}
		if kind == "expired" {
			started, registered, deadline = now.Add(-2*time.Minute), now.Add(-2*time.Minute), now.Add(-time.Minute)
		}
		requestID, operatorID := uuid.New(), int64(42)
		parent := model.JobInstance{JobID: 11, JobName: "host unbound fixture", ExecutorGroupID: 17,
			TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID, Status: model.InstanceRunning,
			StartedAt: &started, DeadlineAt: &deadline, CreatedAt: started, UpdatedAt: started,
			ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "test.recovery", ExecutionMode: model.ExecutionModeNormal,
				RetryIntervalSeconds: 3600, TimeoutSeconds: 300}}
		want := model.TaskRunning
		if kind == "exhausted" {
			want = model.TaskFailed
		}
		if kind == "retry" {
			parent.ExecutionConfig.MaxRetries, want = 1, model.TaskRetryWaiting
		}
		if err := db.Create(&parent).Error; err != nil {
			t.Fatal(err)
		}
		task := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning,
			AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &registered, StartedAt: &started, CreatedAt: started, UpdatedAt: started}
		if kind == "bound" {
			executorID, address := uuid.New(), "127.0.0.1:19001"
			task.ExecutorInstanceID, task.ExecutorAddress = &executorID, &address
		}
		if err := db.Create(&task).Error; err != nil {
			t.Fatal(err)
		}
		fixtures = append(fixtures, fixture{parent: parent, task: task, want: want})
	}
	c, applicationName := jobServiceConfig(t, sqlDB)
	c.UnboundRecovery.PageSize = 2
	c.UnboundRecovery.ScanInterval = 50 * time.Millisecond
	host, err := NewServiceContext(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := host.Close(); err != nil {
			t.Error(err)
		}
	})
	// The configured etcd endpoint is offline. No elected term or direct call to
	// Settler supplies these pre-existing rows; the host's scanner must find them.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		complete := true
		for _, f := range fixtures {
			var task model.JobTask
			var parent model.JobInstance
			if err := db.WithContext(ctx).First(&task, f.task.ID).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.WithContext(ctx).First(&parent, f.parent.ID).Error; err != nil {
				t.Fatal(err)
			}
			complete = complete && task.Status == f.want
			if f.want == model.TaskFailed {
				complete = complete && parent.Status == model.InstanceFailed
			}
		}
		if complete {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("host did not scan all pages and complete eligible attempts")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := host.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixtures {
		var task model.JobTask
		var parent model.JobInstance
		if err := db.First(&task, f.task.ID).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.First(&parent, f.parent.ID).Error; err != nil {
			t.Fatal(err)
		}
		if task.Status != f.want || task.AttemptNo != 1 || task.UsedAttempts != 1 ||
			!task.AttemptRegisteredAt.Equal(*f.task.AttemptRegisteredAt) || !task.StartedAt.Equal(*f.task.StartedAt) ||
			!parent.DeadlineAt.Equal(*f.parent.DeadlineAt) || parent.ExecutionConfig != f.parent.ExecutionConfig {
			t.Fatal("automatic observation changed attempt identity, budget or lifetime")
		}
		if f.want == model.TaskFailed && (task.EndReason == nil || *task.EndReason != model.EndReasonResultUnknown) {
			t.Fatal("exhausted observation did not retain the unknown-result reason")
		}
		if f.want == model.TaskRetryWaiting && (task.NextDispatchAt == nil || !task.NextDispatchAt.After(now.Add(59*time.Minute)) || task.EndReason != nil) {
			t.Fatal("retry candidate did not retain its frozen delay")
		}
		if f.want != model.TaskFailed && parent.Status != model.InstanceRunning {
			t.Fatal("observation scan performed an unrelated parent transition")
		}
	}
	assertNoServiceConnections(t, sqlDB, applicationName)
}
