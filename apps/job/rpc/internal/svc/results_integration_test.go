//go:build integration

package svc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

func TestServiceResultReceiverPersistsAcceptedWorkDuringDrain(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	group, instance, task := serviceResultFixture(t, db)
	c, applicationName := jobServiceConfig(t, sqlDB)
	c.ResultPersistence.Capacity = 1
	c.ResultPersistence.Concurrency = 1
	c.ResultPersistence.SaveTimeout = 3 * time.Second
	s, err := NewServiceContext(c)
	if err != nil {
		t.Fatalf("initialize real result receiver with offline election: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Drain(ctx)
		if err := s.Wait(ctx); err != nil {
			t.Errorf("wait for result service cleanup: %v", err)
			return
		}
		if err := s.Close(); err != nil {
			t.Errorf("close result service: %v", err)
		}
	})
	if s.Results == nil || s.ResultReceiver == nil {
		t.Fatal("service returned before result dependencies were assembled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := &protocol.ReportTaskResultRequest{
		Auth:  &protocol.GroupAuth{GroupName: group.Name, Token: group.Token},
		JobId: instance.JobID, InstanceId: instance.ID, TaskId: task.ID,
		AttemptNo: task.AttemptNo, ExecutorInstanceId: task.ExecutorInstanceID.String(),
		Outcome: protocol.TaskOutcome_SUCCESS, Summary: "service result completed",
		Output: &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte(`9007199254740993`)},
	}

	// A real row lock prevents the consumer from committing. Admission must use
	// the configured one-record capacity without waiting for persistence.
	blocker := db.WithContext(ctx).Begin()
	if blocker.Error != nil {
		t.Fatal(blocker.Error)
	}
	defer blocker.Rollback()
	if err := blocker.Model(&model.JobTask{}).Where("id = ?", task.ID).
		Update("result_summary", "uncommitted service test blocker").Error; err != nil {
		t.Fatal(err)
	}
	reportCtx, reportCancel := context.WithCancel(ctx)
	defer reportCancel()
	if response, err := s.ResultReceiver.ReportTaskResult(reportCtx, request); err != nil || response == nil {
		t.Fatalf("configured receiver did not admit result: response=%v error=%v", response, err)
	}
	reportCancel()
	if _, err := s.ResultReceiver.ReportTaskResult(ctx, request); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("configured one-record capacity error=%v", err)
	}
	var before model.JobTask
	if err := db.WithContext(ctx).Where("id = ?", task.ID).Take(&before).Error; err != nil {
		t.Fatal(err)
	}
	if before.Status != model.TaskRunning || before.ResultData != nil || before.FinishedAt != nil {
		t.Fatal("admission ACK was mistaken for committed persistence")
	}
	s.Stop()
	if _, err := s.ResultReceiver.ReportTaskResult(ctx, request); status.Code(err) != codes.Unavailable {
		t.Fatalf("stopped service did not reject result admission: %v", err)
	}
	if err := blocker.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if err := s.Drain(ctx); err != nil {
		t.Fatalf("drain accepted real result: %v", err)
	}
	if err := s.Wait(ctx); err != nil {
		t.Fatalf("wait for actual result and runtime completion: %v", err)
	}
	if err := s.sqlDB.PingContext(ctx); err != nil || s.etcd.Ctx().Err() != nil {
		t.Fatalf("draining closed shared dependencies: database error=%v", err)
	}
	var saved model.JobTask
	if err := db.WithContext(ctx).Where("id = ?", task.ID).Take(&saved).Error; err != nil {
		t.Fatal(err)
	}
	if saved.Status != model.TaskSucceeded || string(saved.ResultData) != "9007199254740993" ||
		saved.ResultSummary == nil || *saved.ResultSummary != request.Summary ||
		saved.EndReason == nil || *saved.EndReason != model.EndReasonSuccess ||
		saved.FinishedAt == nil || !saved.FinishedAt.Equal(saved.UpdatedAt) {
		t.Fatalf("service did not persist accepted result: status=%d reason=%v", saved.Status, saved.EndReason)
	}
	if saved.AttemptNo != task.AttemptNo || saved.UsedAttempts != task.UsedAttempts ||
		saved.ExecutorInstanceID == nil || *saved.ExecutorInstanceID != *task.ExecutorInstanceID ||
		saved.ExecutorAddress == nil || *saved.ExecutorAddress != *task.ExecutorAddress ||
		saved.StartedAt == nil || !saved.StartedAt.Equal(*task.StartedAt) {
		t.Fatal("service result saving changed attempt accounting or executor binding")
	}
	var parent model.JobInstance
	if err := db.WithContext(ctx).Where("id = ?", instance.ID).Take(&parent).Error; err != nil {
		t.Fatal(err)
	}
	if parent.Status != model.InstanceRunning || parent.FinishedAt != nil {
		t.Fatal("result-only service wiring unexpectedly advanced the parent")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.etcd.Ctx().Err() == nil || s.sqlDB.PingContext(context.Background()) == nil {
		t.Fatal("Close did not release dependencies after result draining")
	}
	assertNoServiceConnections(t, sqlDB, applicationName)
}

func serviceResultFixture(t *testing.T, db *gorm.DB) (model.ExecutorGroup, model.JobInstance, model.JobTask) {
	t.Helper()
	group := model.ExecutorGroup{Name: "service-result-workers", DiscoveryKey: "test.service.result.workers",
		Token: strings.ReplaceAll(uuid.NewString(), "-", "")}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	job := model.JobDefinition{Name: "service result test", ExecutorGroupID: group.ID,
		ProcessorName: "test.service.result", ExecutionMode: model.ExecutionModeNormal,
		CronExpr: "0 * * * * *", MaxRetries: 3, RetryIntervalSeconds: 10, TimeoutSeconds: 900}
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 11, 4, 1, 2, 3, 0, time.UTC)
	deadline := started.Add(15 * time.Minute)
	requestID, operatorID := uuid.New(), int64(1)
	instance := model.JobInstance{JobID: job.ID, JobName: job.Name, ExecutorGroupID: group.ID,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: job.ProcessorName,
			ExecutionMode: model.ExecutionModeNormal, MaxRetries: 3, RetryIntervalSeconds: 10, TimeoutSeconds: 900},
		Status: model.InstanceRunning, StartedAt: &started, DeadlineAt: &deadline, CreatedAt: started, UpdatedAt: started}
	if err := db.Create(&instance).Error; err != nil {
		t.Fatal(err)
	}
	executorID, executorAddress := uuid.New(), "127.0.0.1:9100"
	task := model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning,
		AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &started, StartedAt: &started,
		ExecutorInstanceID: &executorID, ExecutorAddress: &executorAddress, CreatedAt: started, UpdatedAt: started}
	if err := db.Create(&task).Error; err != nil {
		t.Fatal(err)
	}
	return group, instance, task
}
