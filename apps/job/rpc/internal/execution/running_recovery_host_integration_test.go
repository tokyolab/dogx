//go:build integration

package execution_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

func TestServiceContextRecoversRunningOnlyAfterLeadership(t *testing.T) {
	if os.Getenv("DOGX_RUNNING_RECOVERY_HOST_TEST") == "1" {
		testRunningRecoveryHost(t, os.Getenv("DOGX_RUNNING_RECOVERY_HOST_ETCD"))
		return
	}
	// The parent keeps the isolated etcd process alive until all child-owned
	// production components and process-global transport state have exited.
	endpoint := dispatchHostEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestServiceContextRecoversRunningOnlyAfterLeadership$", "-test.v")
	command.Env = append(os.Environ(), "DOGX_RUNNING_RECOVERY_HOST_TEST=1", "DOGX_RUNNING_RECOVERY_HOST_ETCD="+endpoint)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("running recovery host subprocess: %v\n%s", err, output)
	}
	t.Log("production leadership started RUNNING rechecks: missing results timed out, complete results succeeded after deadline, and recent or healthy ordinary/sharded rounds stayed unchanged")
}

func testRunningRecoveryHost(t *testing.T, endpoint string) {
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatal("the parent-owned isolated etcd endpoint is required")
	}
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	c, _ := dispatchHostConfig(t, sqlDB, endpoint)
	c.Dispatch.ScanInterval, c.UnboundRecovery.ScanInterval = time.Hour, time.Hour
	c.InstanceRecovery.Interval, c.InstanceRecovery.PageSize = 50*time.Millisecond, 2
	c.Stopping.BatchSize, c.Stopping.MaxBatches = 2, 1
	group := model.ExecutorGroup{Name: "running-recovery-workers", DiscoveryKey: c.Scheduler.Prefix + "/executor",
		Token: "00000000000040008000000000000001"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	auth, err := jobexecutor.NewAuthenticator(group.Name, group.Token)
	if err != nil {
		t.Fatal(err)
	}
	remote := &stopHostProtocolServer{auth: auth, response: "success", entered: make(chan struct{}, 16), release: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	protocol.RegisterExecutorServer(server, remote)
	serving := make(chan struct{})
	go func() { defer close(serving); _ = server.Serve(listener) }()
	t.Cleanup(func() { close(remote.release); server.Stop(); <-serving })
	var now time.Time
	if err := db.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	timedOut := createRunningRecoveryRound(t, db, group, address, "missing result", model.ExecutionModeNormal,
		now.Add(-10*time.Minute), now.Add(-5*time.Minute), model.TaskRunning)
	completed := createRunningRecoveryRound(t, db, group, address, "complete after deadline", model.ExecutionModeNormal,
		now.Add(-10*time.Minute), now.Add(-5*time.Minute), model.TaskSucceeded)
	recent := createRunningRecoveryRound(t, db, group, address, "recent expired", model.ExecutionModeNormal,
		now.Add(-2*time.Minute), now.Add(-time.Minute), model.TaskRunning)
	healthy := createRunningRecoveryRound(t, db, group, address, "healthy long run", model.ExecutionModeNormal,
		now.Add(-10*time.Minute), now.Add(30*time.Minute), model.TaskRunning)
	sharded := createRunningRecoveryRound(t, db, group, address, "healthy preparation", model.ExecutionModeSharded,
		now.Add(-10*time.Minute), now.Add(30*time.Minute), model.TaskRunning)
	installRunningRecoveryAudit(t, db)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	session, err := concurrency.NewSession(client, concurrency.WithTTL(15))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	leader := concurrency.NewElection(session, c.Scheduler.Prefix+"/scheduler/election")
	campaignCtx, cancelCampaign := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCampaign()
	if err := leader.Campaign(campaignCtx, "test-owned-leader"); err != nil {
		t.Fatal(err)
	}
	host, err := svc.NewServiceContext(c)
	if err != nil {
		t.Fatalf("initialize production running recovery host: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
		defer cancel()
		_ = host.Drain(ctx)
		if err := host.Wait(ctx); err != nil {
			t.Errorf("wait for running recovery host: %v", err)
			return
		}
		if err := host.Close(); err != nil {
			t.Errorf("close running recovery host: %v", err)
		}
	})
	dispatchHostEventually(t, "running recovery host waiting behind the elected test node", func(ctx context.Context) bool {
		candidates, err := client.Get(ctx, c.Scheduler.Prefix+"/scheduler/election/", clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.Kvs) != 2 {
			return false
		}
		var testCandidate, hostCandidate bool
		for _, candidate := range candidates.Kvs {
			if candidate.Lease == 0 {
				t.Fatal("scheduler candidate has no election lease")
			}
			testCandidate = testCandidate || string(candidate.Value) == "test-owned-leader"
			hostCandidate = hostCandidate || string(candidate.Value) == "scheduler"
		}
		return testCandidate && hostCandidate
	})
	assertDispatchHostFollower(t, leader)
	ticker := time.NewTicker(c.InstanceRecovery.Interval)
	defer ticker.Stop()
	for range 3 {
		<-ticker.C
		for _, round := range []runningRecoveryRound{timedOut, completed, recent, healthy, sharded} {
			assertRunningRecoveryUnchanged(t, db, round)
		}
		if len(remote.snapshot()) != 0 {
			t.Fatal("follower notified an existing running round without a local completion hint")
		}
	}
	assertDispatchHostFollower(t, leader)
	resignCtx, cancelResign := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelResign()
	if err := leader.Resign(resignCtx); err != nil {
		t.Fatal(err)
	}
	dispatchHostEventually(t, "running recovery host leadership", func(ctx context.Context) bool {
		elected, err := leader.Leader(ctx)
		return err == nil && len(elected.Kvs) == 1 && string(elected.Kvs[0].Value) == "scheduler"
	})
	var timeoutParent, completedParent model.JobInstance
	// No API call, direct Hint or test-owned recovery loop runs here. The new
	// leader must discover persisted parents through production runtime wiring.
	dispatchHostEventually(t, "old running rounds reaching timeout or normal completion", func(ctx context.Context) bool {
		if err := db.WithContext(ctx).First(&timeoutParent, timedOut.parent.ID).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.WithContext(ctx).First(&completedParent, completed.parent.ID).Error; err != nil {
			t.Fatal(err)
		}
		return timeoutParent.Status == model.InstanceFailed && completedParent.Status == model.InstanceSucceeded
	})
	if timeoutParent.EndReason == nil || *timeoutParent.EndReason != model.EndReasonInstanceTimeout || timeoutParent.FinishedAt == nil ||
		timeoutParent.StartedAt == nil || !timeoutParent.StartedAt.Equal(*timedOut.parent.StartedAt) ||
		timeoutParent.DeadlineAt == nil || !timeoutParent.DeadlineAt.Equal(*timedOut.parent.DeadlineAt) {
		t.Fatal("timeout recovery lost its reason or changed the frozen execution lifetime")
	}
	if completedParent.EndReason == nil || *completedParent.EndReason != model.EndReasonSuccess || completedParent.FinishedAt == nil ||
		completedParent.ResultSummary == nil || *completedParent.ResultSummary != *completed.task.ResultSummary ||
		completedParent.DeadlineAt == nil || !completedParent.DeadlineAt.Equal(*completed.parent.DeadlineAt) {
		t.Fatal("complete results after deadline were not preserved as normal success")
	}
	var timeoutTask, completedTask model.JobTask
	if err := db.First(&timeoutTask, timedOut.task.ID).Error; err != nil {
		t.Fatal(err)
	}
	if timeoutTask.Status != model.TaskCanceled || timeoutTask.EndReason == nil || *timeoutTask.EndReason != model.EndReasonInstanceTimeout ||
		timeoutTask.FinishedAt == nil || timeoutTask.AttemptNo != timedOut.task.AttemptNo || timeoutTask.UsedAttempts != timedOut.task.UsedAttempts ||
		timeoutTask.ExecutorInstanceID == nil || *timeoutTask.ExecutorInstanceID != *timedOut.task.ExecutorInstanceID ||
		timeoutTask.ExecutorAddress == nil || *timeoutTask.ExecutorAddress != address {
		t.Fatal("timeout convergence did not cancel the child while preserving its attempt and binding")
	}
	if err := db.First(&completedTask, completed.task.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(completedTask, completed.task) {
		t.Fatal("normal completion after deadline rewrote the successful Task")
	}
	assertRunningRecoveryTransitions(t, db, timedOut.parent.ID, []model.InstanceStatus{model.InstanceStopping, model.InstanceFailed})
	assertRunningRecoveryTransitions(t, db, completed.parent.ID, []model.InstanceStatus{model.InstanceSucceeded})
	dispatchHostEventually(t, "best-effort timeout notification to saved address", func(context.Context) bool {
		return len(remote.snapshot()) > 0
	})
	// Additional discovery rounds must not terminate healthy work, advance an
	// unfinished preparation, or bypass the minimum age because a deadline elapsed.
	for range 3 {
		<-ticker.C
		for _, round := range []runningRecoveryRound{recent, healthy, sharded} {
			assertRunningRecoveryUnchanged(t, db, round)
		}
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancelShutdown()
	if err := host.Drain(shutdown); err != nil {
		t.Fatal(err)
	}
	if err := host.Wait(shutdown); err != nil {
		t.Fatal(err)
	}
	calls := remote.snapshot()
	if len(calls) != 1 || calls[0].instanceID != timedOut.parent.ID || calls[0].reason != protocol.CancelReason_INSTANCE_TIMEOUT || !calls[0].authenticated {
		t.Fatalf("new timeout decision did not produce one authenticated Instance notification: calls=%v", calls)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
}

type runningRecoveryRound struct {
	parent model.JobInstance
	task   model.JobTask
}

func createRunningRecoveryRound(t *testing.T, db *gorm.DB, group model.ExecutorGroup, address, name string,
	mode model.ExecutionMode, started, deadline time.Time, taskStatus model.TaskStatus) runningRecoveryRound {
	t.Helper()
	definition := model.JobDefinition{Name: name, ExecutorGroupID: group.ID, ProcessorName: "test.recovery",
		ExecutionMode: mode, CronExpr: "0 * * * * *", Status: basemodel.RecordStatusDisabled,
		BlockPolicy: model.BlockParallel, RetryIntervalSeconds: 10, TimeoutSeconds: int32(deadline.Sub(started).Seconds())}
	if err := db.Create(&definition).Error; err != nil {
		t.Fatal(err)
	}
	requestID, operatorID := uuid.New(), int64(42)
	parent := model.JobInstance{JobID: definition.ID, JobName: definition.Name, ExecutorGroupID: group.ID,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID, Status: model.InstanceRunning,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: definition.ProcessorName,
			ExecutionMode: mode, RetryIntervalSeconds: 10, TimeoutSeconds: definition.TimeoutSeconds},
		StartedAt: &started, DeadlineAt: &deadline, CreatedAt: started, UpdatedAt: started}
	if err := db.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	executorID := uuid.New()
	task := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeOrdinary, Status: taskStatus,
		AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &started, StartedAt: &started,
		ExecutorInstanceID: &executorID, ExecutorAddress: &address, CreatedAt: started, UpdatedAt: started}
	if mode == model.ExecutionModeSharded {
		task.TaskType = model.TaskTypePrepare
	}
	if taskStatus == model.TaskSucceeded {
		finished, summary, reason := deadline.Add(time.Second), "Recovered the saved successful result.", model.EndReasonSuccess
		task.FinishedAt, task.ResultSummary, task.EndReason = &finished, &summary, &reason
		task.ResultData, task.UpdatedAt = model.TaskData(`{"saved":true}`), finished
	}
	if err := db.Create(&task).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&parent, parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&task, task.ID).Error; err != nil {
		t.Fatal(err)
	}
	return runningRecoveryRound{parent: parent, task: task}
}

func assertRunningRecoveryUnchanged(t *testing.T, db *gorm.DB, round runningRecoveryRound) {
	t.Helper()
	var parent model.JobInstance
	var task model.JobTask
	if err := db.First(&parent, round.parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&task, round.task.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parent, round.parent) || !reflect.DeepEqual(task, round.task) {
		t.Fatalf("running recovery changed an ineligible round: instance_id=%d", round.parent.ID)
	}
}

func installRunningRecoveryAudit(t *testing.T, db *gorm.DB) {
	t.Helper()
	// Persisted transitions prove that timeout first records STOPPING and later
	// commits the terminal parent, instead of inferring that sequence from a poll.
	for _, statement := range []string{
		`CREATE TABLE running_recovery_audit (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		 instance_id BIGINT NOT NULL, status SMALLINT NOT NULL, transaction_id BIGINT NOT NULL)`,
		`CREATE FUNCTION record_running_recovery_transition() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		  IF NEW.status <> OLD.status THEN
		    INSERT INTO running_recovery_audit (instance_id, status, transaction_id)
		    VALUES (NEW.id, NEW.status, txid_current());
		  END IF;
		  RETURN NEW;
		END; $$`,
		`CREATE TRIGGER running_recovery_audit AFTER UPDATE ON job_instance
		 FOR EACH ROW EXECUTE FUNCTION record_running_recovery_transition()`,
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func assertRunningRecoveryTransitions(t *testing.T, db *gorm.DB, id int64, expected []model.InstanceStatus) {
	t.Helper()
	var transitions []struct {
		Status        model.InstanceStatus
		TransactionID int64
	}
	if err := db.Table("running_recovery_audit").Select("status", "transaction_id").Where("instance_id = ?", id).
		Order("id").Find(&transitions).Error; err != nil {
		t.Fatal(err)
	}
	if len(transitions) != len(expected) {
		t.Fatalf("unexpected parent transition count: instance_id=%d transitions=%v", id, transitions)
	}
	for index, transition := range transitions {
		if transition.Status != expected[index] || transition.TransactionID <= 0 ||
			index > 0 && transition.TransactionID == transitions[index-1].TransactionID {
			t.Fatalf("parent transitions did not preserve independent commits: instance_id=%d transitions=%v", id, transitions)
		}
	}
}
