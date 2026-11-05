//go:build integration

package execution

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/groupcache"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

func TestReceiverQueuePersistsWithRealPostgres(t *testing.T) {
	if queueReceiverSubprocess(t, "DOGX_RESULT_QUEUE_PG_TEST", "TestReceiverQueuePersistsWithRealPostgres") {
		return
	}
	t.Cleanup(func() { _ = logx.Close() })
	db, _ := testutil.OpenMigratedPostgres(t)
	group, otherGroup, instance, task := receiverQueueFixture(t, db)
	groups, err := repository.NewExecutorGroupRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := groupcache.New(context.Background(), groups, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewCachedGroupAuthenticator(cache)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := repository.NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	hints := make(chan int64, 4)
	saver, err := taskresult.NewSaver(tasks, taskresult.WithInstanceHint(func(id int64) {
		select {
		case hints <- id:
		default:
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	attempts := make(chan receiverQueueSaveAttempt, 16)
	queue, err := taskresult.NewQueue(&receiverQueueObservedSaver{next: saver, attempts: attempts}, taskresult.QueueConfig{Capacity: 4, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopReceiverQueue(t, queue) })
	if err := queue.Start(); err != nil {
		t.Fatal(err)
	}
	receiver := testReceiver(t, auth, queue)
	var logs transportLogBuffer
	client := queueReceiverClient(t, receiver)
	// Install capture after go-zero has applied process-level server logging.
	logx.SetWriter(logx.NewWriter(&logs))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := validReceiverRequest()
	request.Auth.GroupName = group.Name
	request.JobId, request.InstanceId, request.TaskId, request.AttemptNo = instance.JobID, instance.ID, task.ID, task.AttemptNo
	request.Summary = "synthetic-private-queue-result"
	request.Output = &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte(`9007199254740993`)}

	// A separate transaction prevents persistence while the real gRPC adapter
	// admits the result. The ACK must not wait for this database row lock.
	blocker := db.WithContext(ctx).Begin()
	if blocker.Error != nil {
		t.Fatal(blocker.Error)
	}
	defer blocker.Rollback()
	if err := blocker.Model(&model.JobTask{}).Where("id = ?", task.ID).
		Update("result_summary", "uncommitted test blocker").Error; err != nil {
		t.Fatal(err)
	}
	rpcCtx, rpcCancel := context.WithCancel(ctx)
	if _, err := client.ReportTaskResult(rpcCtx, request); err != nil {
		rpcCancel()
		t.Fatalf("real queue admission before persistence: %v", err)
	}
	rpcCancel()
	beforeRelease := receiverQueueReadTask(t, db, task.ID)
	if beforeRelease.Status != model.TaskRunning || beforeRelease.ResultData != nil || beforeRelease.FinishedAt != nil {
		t.Fatal("RPC ACK was mistaken for a saved result")
	}
	select {
	case <-hints:
		t.Fatal("Instance hint was published before the Task write committed")
	default:
	}
	if err := blocker.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	waitReceiverQueueHint(t, ctx, hints, instance.ID)
	saved := receiverQueueReadTask(t, db, task.ID)
	if saved.Status != model.TaskSucceeded || string(saved.ResultData) != "9007199254740993" ||
		saved.ResultSummary == nil || *saved.ResultSummary != request.Summary || saved.EndReason == nil || *saved.EndReason != model.EndReasonSuccess ||
		saved.FinishedAt == nil || !saved.FinishedAt.Equal(saved.UpdatedAt) || saved.ExecutorInstanceID != nil || saved.ExecutorAddress != nil {
		t.Fatalf("receiver/queue/saver did not persist the expected result: status=%d reason=%v", saved.Status, saved.EndReason)
	}
	if saved.AttemptNo != task.AttemptNo || saved.UsedAttempts != task.UsedAttempts || saved.StartedAt == nil || !saved.StartedAt.Equal(*task.StartedAt) {
		t.Fatal("result persistence changed attempt accounting or registration")
	}
	// Replay uses the same real queue and saver. It may hint a recheck, but it
	// cannot rewrite the committed outcome or its processing timestamp.
	if _, err := client.ReportTaskResult(ctx, request); err != nil {
		t.Fatalf("duplicate report admission: %v", err)
	}
	waitReceiverQueueHint(t, ctx, hints, instance.ID)
	if after := receiverQueueReadTask(t, db, task.ID); !reflect.DeepEqual(saved, after) {
		t.Fatal("duplicate report changed the saved result")
	}

	// Fail one real database UPDATE after admission. The queue must retain the
	// same accepted report and retry it once storage becomes usable, without a
	// second RPC or a new execution attempt. The trigger exists only in this
	// isolated schema and is restricted to the newly created test Task.
	recoveryInstance, recoveryTask := receiverQueueRecoveryTask(t, db, instance, task)
	functionSQL := fmt.Sprintf(`CREATE FUNCTION reject_queue_test_result() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id = %d THEN
  RAISE EXCEPTION 'synthetic-sensitive-database-detail';
 END IF;
 RETURN NEW;
END;
$$`, recoveryTask.ID)
	if err := db.Exec(functionSQL).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("CREATE TRIGGER reject_queue_test_result BEFORE UPDATE ON job_task FOR EACH ROW EXECUTE FUNCTION reject_queue_test_result()").Error; err != nil {
		t.Fatal(err)
	}
	recoveryRequest := validReceiverRequest()
	recoveryRequest.Auth.GroupName = group.Name
	recoveryRequest.JobId, recoveryRequest.InstanceId, recoveryRequest.TaskId, recoveryRequest.AttemptNo = recoveryInstance.JobID, recoveryInstance.ID, recoveryTask.ID, recoveryTask.AttemptNo
	recoveryRequest.Summary = "synthetic-recovery-result"
	recoveryRequest.Output = &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte(`true`)}
	if _, err := client.ReportTaskResult(ctx, recoveryRequest); err != nil {
		t.Fatalf("temporary database failure prevented queue ACK: %v", err)
	}
	failed := waitReceiverQueueAttempt(t, ctx, attempts, recoveryTask.ID)
	if !failed.failed || failed.applied {
		t.Fatalf("test storage fault did not reach the real Saver: %+v", failed)
	}
	if beforeRetry := receiverQueueReadTask(t, db, recoveryTask.ID); beforeRetry.Status != model.TaskRunning || beforeRetry.ResultData != nil {
		t.Fatal("failed persistence altered the Task")
	}
	select {
	case <-hints:
		t.Fatal("failed persistence published an Instance hint")
	default:
	}
	if err := db.Exec("DROP TRIGGER reject_queue_test_result ON job_task").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DROP FUNCTION reject_queue_test_result()").Error; err != nil {
		t.Fatal(err)
	}
	waitReceiverQueueHint(t, ctx, hints, recoveryInstance.ID)
	recovered := receiverQueueReadTask(t, db, recoveryTask.ID)
	if recovered.Status != model.TaskSucceeded || string(recovered.ResultData) != "true" ||
		recovered.AttemptNo != recoveryTask.AttemptNo || recovered.UsedAttempts != recoveryTask.UsedAttempts ||
		recovered.FinishedAt == nil || !recovered.FinishedAt.Equal(recovered.UpdatedAt) {
		t.Fatal("retained report did not recover after storage became available")
	}
	// Authentication is real, but the authenticated group still cannot mutate
	// another group's Task. A valid admission ACK does not bypass Saver ownership.
	request.Auth.GroupName = otherGroup.Name
	request.Summary = "synthetic-cross-group-result"
	if _, err := client.ReportTaskResult(ctx, request); err != nil {
		t.Fatalf("authenticated cross-group report admission: %v", err)
	}
	queue.Stop()
	if err := queue.Drain(ctx); err != nil {
		t.Fatalf("drain real result persistence: %v", err)
	}
	if err := queue.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if after := receiverQueueReadTask(t, db, task.ID); !reflect.DeepEqual(saved, after) {
		t.Fatal("cross-group result changed persisted facts")
	}
	select {
	case id := <-hints:
		t.Fatalf("unverified ownership triggered Instance hint %d", id)
	default:
	}
	var parent model.JobInstance
	if err := db.Where("id = ?", instance.ID).Take(&parent).Error; err != nil {
		t.Fatal(err)
	}
	if parent.Status != model.InstanceRunning || parent.FinishedAt != nil {
		t.Fatal("component persistence test unexpectedly advanced the parent")
	}
	for _, private := range []string{receiverTestToken, "synthetic-private-queue-result", "synthetic-cross-group-result", "synthetic-recovery-result", "synthetic-sensitive-database-detail", "9007199254740993"} {
		if strings.Contains(logs.String(), private) {
			t.Fatal("result transport or persistence log exposed credentials or content")
		}
	}
}

func receiverQueueRecoveryTask(t *testing.T, db *gorm.DB, instance model.JobInstance, task model.JobTask) (model.JobInstance, model.JobTask) {
	t.Helper()
	requestID := uuid.New()
	instance.ID, instance.RequestID = 0, &requestID
	if err := db.Create(&instance).Error; err != nil {
		t.Fatal(err)
	}
	task.ID, task.InstanceID = 0, instance.ID
	if err := db.Create(&task).Error; err != nil {
		t.Fatal(err)
	}
	return instance, task
}

func receiverQueueFixture(t *testing.T, db *gorm.DB) (model.ExecutorGroup, model.ExecutorGroup, model.JobInstance, model.JobTask) {
	t.Helper()
	group := model.ExecutorGroup{Name: "queue-workers", DiscoveryKey: "test.queue.rpc", Token: receiverTestToken}
	other := model.ExecutorGroup{Name: "other-queue-workers", DiscoveryKey: "test.other.queue.rpc", Token: receiverTestToken}
	for _, value := range []*model.ExecutorGroup{&group, &other} {
		if err := db.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	job := model.JobDefinition{Name: "queue persistence test", ExecutorGroupID: group.ID,
		ProcessorName: "test.queue", ExecutionMode: model.ExecutionModeNormal,
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
	task := model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning,
		AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &started, StartedAt: &started,
		CreatedAt: started, UpdatedAt: started}
	if err := db.Create(&task).Error; err != nil {
		t.Fatal(err)
	}
	return group, other, instance, task
}

func receiverQueueReadTask(t *testing.T, db *gorm.DB, taskID int64) model.JobTask {
	t.Helper()
	var task model.JobTask
	if err := db.Where("id = ?", taskID).Take(&task).Error; err != nil {
		t.Fatal(err)
	}
	return task
}

func waitReceiverQueueHint(t *testing.T, ctx context.Context, hints <-chan int64, instanceID int64) {
	t.Helper()
	select {
	case id := <-hints:
		if id != instanceID {
			t.Fatalf("hint=%d want=%d", id, instanceID)
		}
	case <-ctx.Done():
		t.Fatal("real persistence did not publish a committed Instance hint")
	}
}

type receiverQueueSaveAttempt struct {
	taskID  int64
	applied bool
	failed  bool
}

// receiverQueueObservedSaver delegates unchanged to the real Saver. It exposes
// only completion facts to synchronize the test, never errors or report content.
type receiverQueueObservedSaver struct {
	next     *taskresult.Saver
	attempts chan<- receiverQueueSaveAttempt
}

func (s *receiverQueueObservedSaver) Save(ctx context.Context, report taskresult.Report) (taskresult.SaveResult, error) {
	result, err := s.next.Save(ctx, report)
	select {
	case s.attempts <- receiverQueueSaveAttempt{taskID: report.TaskID, applied: result.Applied, failed: err != nil}:
	default:
	}
	return result, err
}

func waitReceiverQueueAttempt(t *testing.T, ctx context.Context, attempts <-chan receiverQueueSaveAttempt, taskID int64) receiverQueueSaveAttempt {
	t.Helper()
	for {
		select {
		case attempt := <-attempts:
			if attempt.taskID == taskID {
				return attempt
			}
		case <-ctx.Done():
			t.Fatal("real Saver did not finish the expected attempt")
			return receiverQueueSaveAttempt{}
		}
	}
}
