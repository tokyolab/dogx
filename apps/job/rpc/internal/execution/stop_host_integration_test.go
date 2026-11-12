//go:build integration

package execution_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/logic"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

func TestServiceContextRecoversStoppingOnlyAfterLeadership(t *testing.T) {
	if os.Getenv("DOGX_STOP_RECOVERY_HOST_TEST") == "1" {
		testStopRecoveryHost(t, os.Getenv("DOGX_STOP_RECOVERY_HOST_ETCD"))
		return
	}
	// The parent owns isolated etcd through the child's complete lifetime.
	// Process-global transport state cannot escape this host integration test.
	endpoint := dispatchHostEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestServiceContextRecoversStoppingOnlyAfterLeadership$", "-test.v")
	command.Env = append(os.Environ(), "DOGX_STOP_RECOVERY_HOST_TEST=1", "DOGX_STOP_RECOVERY_HOST_ETCD="+endpoint)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("stopping recovery host subprocess: %v\n%s", err, output)
	}
	t.Log("production host kept STOPPING untouched while following, then recovered its database state after election without sending cancellation RPCs")
}

func testStopRecoveryHost(t *testing.T, endpoint string) {
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatal("the parent-owned isolated etcd endpoint is required")
	}
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	c, _ := dispatchHostConfig(t, sqlDB, endpoint)
	c.Dispatch.ScanInterval, c.UnboundRecovery.ScanInterval = time.Hour, time.Hour
	c.Stopping.BatchSize, c.Stopping.MaxBatches = 2, 1
	c.Stopping.RecoveryPageSize, c.InstanceRecovery.Interval = 1, 50*time.Millisecond
	group := model.ExecutorGroup{Name: "stop-recovery-workers", DiscoveryKey: c.Scheduler.Prefix + "/executor", Token: "00000000000040008000000000000001"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	auth, err := jobexecutor.NewAuthenticator(group.Name, group.Token)
	if err != nil {
		t.Fatal(err)
	}
	remote := &stopHostProtocolServer{auth: auth, response: "success", entered: make(chan struct{}, 8), release: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	protocol.RegisterExecutorServer(server, remote)
	serving := make(chan struct{})
	go func() { defer close(serving); _ = server.Serve(listener) }()
	t.Cleanup(func() { close(remote.release); server.Stop(); <-serving })
	parent, terminal := stopHostFixture(t, db, group, listener.Addr().String())
	if err := db.Model(&model.JobInstance{}).Where("id = ?", parent.ID).Updates(map[string]any{
		"status": model.InstanceStopping, "end_reason": model.EndReasonManualStop,
		"result_summary": "Persisted stop decision awaiting recovery.",
	}).Error; err != nil {
		t.Fatal(err)
	}
	installStopHostBatchAudit(t, db)
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
		t.Fatalf("initialize production stopping recovery host: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = host.Drain(ctx)
		if err := host.Wait(ctx); err != nil {
			t.Errorf("wait for stopping recovery host: %v", err)
			return
		}
		if err := host.Close(); err != nil {
			t.Errorf("close stopping recovery host: %v", err)
		}
	})
	// Seeing both candidacies proves that the host has reached the real
	// election boundary before the follower-side assertions begin.
	dispatchHostEventually(t, "production host waiting behind the elected test node", func(ctx context.Context) bool {
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
		var current model.JobInstance
		if err := db.First(&current, parent.ID).Error; err != nil {
			t.Fatal(err)
		}
		var unfinished int64
		if err := db.Model(&model.JobTask{}).Where("instance_id = ? AND status IN (1, 2, 3)", parent.ID).Count(&unfinished).Error; err != nil {
			t.Fatal(err)
		}
		if current.Status != model.InstanceStopping || current.FinishedAt != nil || unfinished != 5 || len(remote.snapshot()) != 0 {
			t.Fatal("follower processed or remotely notified a persisted STOPPING Instance without a local hint")
		}
	}
	assertDispatchHostFollower(t, leader)
	resignCtx, cancelResign := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelResign()
	if err := leader.Resign(resignCtx); err != nil {
		t.Fatal(err)
	}
	dispatchHostEventually(t, "production scheduler leadership", func(ctx context.Context) bool {
		elected, err := leader.Leader(ctx)
		return err == nil && len(elected.Kvs) == 1 && string(elected.Kvs[0].Value) == "scheduler"
	})
	var finished model.JobInstance
	// No management call or direct Hint occurs in this test. Only the
	// production WithInstanceRecovery wiring can discover this existing parent.
	dispatchHostEventually(t, "elected host recovering persisted stopping work", func(ctx context.Context) bool {
		if err := db.WithContext(ctx).First(&finished, parent.ID).Error; err != nil {
			t.Fatal(err)
		}
		return finished.Status == model.InstanceCanceled
	})
	if finished.FinishedAt == nil || finished.EndReason == nil || *finished.EndReason != model.EndReasonManualStop ||
		finished.ResultSummary == nil || *finished.ResultSummary != "Persisted stop decision awaiting recovery." {
		t.Fatal("elected recovery lost the original stopping decision")
	}
	assertStopHostTasksAndBatches(t, db, parent.ID, terminal)
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := host.Drain(shutdown); err != nil {
		t.Fatal(err)
	}
	if err := host.Wait(shutdown); err != nil {
		t.Fatal(err)
	}
	if len(remote.snapshot()) != 0 {
		t.Fatal("database recovery incorrectly replayed a best-effort remote cancellation notification")
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceContextStopsPersistedTasksAndNotifiesSavedAddress(t *testing.T) {
	for _, response := range []string{"success", "permanent refusal", "blocked unavailable"} {
		t.Run(response, func(t *testing.T) {
			db, sqlDB := testutil.OpenMigratedPostgres(t)
			// Reserve a loopback endpoint without serving etcd. The host cannot
			// become leader; direct stop notification uses the persisted address.
			offline, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = offline.Close() })
			c, _ := dispatchHostConfig(t, sqlDB, "http://"+offline.Addr().String())
			c.Dispatch.ScanInterval, c.UnboundRecovery.ScanInterval = time.Hour, time.Hour
			c.Stopping.BatchSize, c.Stopping.MaxBatches = 2, 1
			c.CancelNotification.InstanceCapacity, c.CancelNotification.PageSize = 2, 1
			c.CancelNotification.TargetCapacity, c.CancelNotification.Concurrency = 2, 1
			group := model.ExecutorGroup{Name: "stop-host-workers", DiscoveryKey: c.Scheduler.Prefix + "/executor", Token: "00000000000040008000000000000001"}
			if err := db.Create(&group).Error; err != nil {
				t.Fatal(err)
			}
			auth, err := jobexecutor.NewAuthenticator(group.Name, group.Token)
			if err != nil {
				t.Fatal(err)
			}
			executor := &stopHostProtocolServer{auth: auth, response: response, entered: make(chan struct{}, 8), release: make(chan struct{})}
			var releaseOnce sync.Once
			releaseExecutor := func() { releaseOnce.Do(func() { close(executor.release) }) }
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			// Cleanup releases the test gates before Stop, then waits for the
			// remote handlers themselves, not just their client connections.
			server := grpc.NewServer(grpc.WaitForHandlers(true))
			protocol.RegisterExecutorServer(server, executor)
			serving := make(chan struct{})
			go func() { defer close(serving); _ = server.Serve(listener) }()
			t.Cleanup(func() {
				releaseExecutor()
				server.Stop()
				<-serving
			})
			parent, terminal := stopHostFixture(t, db, group, listener.Addr().String())
			installStopHostBatchAudit(t, db)
			host, err := svc.NewServiceContext(c)
			if err != nil {
				t.Fatalf("initialize production stop host with offline election: %v", err)
			}
			t.Cleanup(func() {
				releaseExecutor()
				ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
				defer cancel()
				_ = host.Drain(ctx)
				if err := host.Wait(ctx); err != nil {
					t.Errorf("wait for stopping host cleanup: %v", err)
					return
				}
				if err := host.Close(); err != nil {
					t.Errorf("close stopping host: %v", err)
				}
			})
			if host.Stops == nil || host.CancelNotifications == nil || host.AfterInstanceStopped == nil {
				t.Fatal("production host returned without its stopping components")
			}
			var handoffs atomic.Int32
			forward := host.AfterInstanceStopped
			// Observe the real handoff without replacing either production queue.
			// This distinguishes repeated management requests from transport retries
			// even when the first target is still retained by the notifier.
			host.AfterInstanceStopped = func(decision instancestop.Decision) {
				handoffs.Add(1)
				forward(decision)
			}
			requestCtx, cancelRequest := context.WithTimeout(context.Background(), 5*time.Second)
			stopped, err := logic.NewStopInstanceLogic(requestCtx, host).StopInstance(&job.StopInstanceRequest{Id: parent.ID})
			cancelRequest()
			if err != nil || stopped == nil || stopped.InstanceId != parent.ID || stopped.Status != int32(model.InstanceStopping) ||
				stopped.EndReason == nil || *stopped.EndReason != model.EndReasonManualStop {
				t.Fatalf("manual stop did not return its committed STOPPING decision: %v", err)
			}
			select {
			case <-executor.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("production notification did not call the saved executor address")
			}
			var finished model.JobInstance
			dispatchHostEventually(t, "automatic stopping parent and child convergence", func(ctx context.Context) bool {
				if err := db.WithContext(ctx).First(&finished, parent.ID).Error; err != nil {
					t.Fatal(err)
				}
				return finished.Status == model.InstanceCanceled
			})
			if response == "blocked unavailable" && executor.active.Load() == 0 {
				t.Fatal("database convergence was not observed while the remote handler remained blocked")
			}
			if finished.EndReason == nil || *finished.EndReason != model.EndReasonManualStop || finished.FinishedAt == nil ||
				finished.StartedAt == nil || !finished.StartedAt.Equal(*parent.StartedAt) ||
				finished.DeadlineAt == nil || !finished.DeadlineAt.Equal(*parent.DeadlineAt) {
				t.Fatal("database stop changed the frozen reason or execution lifetime")
			}
			assertStopHostTasksAndBatches(t, db, parent.ID, terminal)
			queryCtx, cancelQuery := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelQuery()
			for range 2 {
				repeated, err := logic.NewStopInstanceLogic(queryCtx, host).StopInstance(&job.StopInstanceRequest{Id: parent.ID})
				if err != nil || repeated == nil || repeated.Status != int32(model.InstanceCanceled) ||
					repeated.EndReason == nil || *repeated.EndReason != model.EndReasonManualStop {
					t.Fatalf("repeated stop did not preserve terminal facts: %v", err)
				}
			}
			if handoffs.Load() != 1 {
				t.Fatal("repeated manual stop published a new cancellation notification")
			}
			releaseExecutor()
			shutdown, cancelShutdown := context.WithTimeout(context.Background(), 7*time.Second)
			defer cancelShutdown()
			if err := host.Drain(shutdown); err != nil {
				t.Fatalf("drain completed database stop and bounded notifications: %v", err)
			}
			if err := host.Wait(shutdown); err != nil {
				t.Fatal(err)
			}
			calls := executor.snapshot()
			if len(calls) == 0 || len(calls) > 3 || response != "blocked unavailable" && len(calls) != 1 {
				t.Fatalf("one saved address produced %d remote calls for %s", len(calls), response)
			}
			for _, call := range calls {
				if call.instanceID != parent.ID || call.reason != protocol.CancelReason_MANUAL_STOP || !call.authenticated {
					t.Fatal("direct cancellation changed Instance identity, reason or group credentials")
				}
			}
			// A successful signal or exhausted transport retries cannot rewrite
			// database stopping facts after the parent has already converged.
			var after model.JobInstance
			if err := db.First(&after, parent.ID).Error; err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, finished) {
				t.Fatal("remote notification or repeated stop changed the terminal parent")
			}
			if err := host.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// stopHostFixture provides persisted shard rows to exercise type-independent
// stopping. It does not claim shard generation or business execution is running.
func stopHostFixture(t *testing.T, db *gorm.DB, group model.ExecutorGroup, address string) (model.JobInstance, model.JobTask) {
	t.Helper()
	definition := model.JobDefinition{Name: "host stopping", ExecutorGroupID: group.ID, ProcessorName: "test.stop",
		ExecutionMode: model.ExecutionModeSharded, CronExpr: "0 * * * * *", Status: basemodel.RecordStatusDisabled,
		BlockPolicy: model.BlockParallel, RetryIntervalSeconds: 10, TimeoutSeconds: 3600}
	if err := db.Create(&definition).Error; err != nil {
		t.Fatal(err)
	}
	var now time.Time
	if err := db.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		t.Fatal(err)
	}
	started := now.Add(-time.Minute)
	deadline, requestID := started.Add(time.Hour), uuid.New()
	operatorID := int64(42)
	parent := model.JobInstance{JobID: definition.ID, JobName: definition.Name, ExecutorGroupID: group.ID,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID, Status: model.InstanceRunning,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: definition.ProcessorName,
			ExecutionMode: model.ExecutionModeSharded, RetryIntervalSeconds: 10, TimeoutSeconds: 3600},
		StartedAt: &started, DeadlineAt: &deadline, CreatedAt: started, UpdatedAt: started}
	if err := db.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	executorID := uuid.New()
	for range 5 {
		task := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeShard, Status: model.TaskRunning,
			AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &started, StartedAt: &started,
			ExecutorInstanceID: &executorID, ExecutorAddress: &address, CreatedAt: started, UpdatedAt: started}
		if err := db.Create(&task).Error; err != nil {
			t.Fatal(err)
		}
	}
	summary, reason := "already successful", model.EndReasonSuccess
	terminal := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeShard, Status: model.TaskSucceeded,
		AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &started, StartedAt: &started, FinishedAt: &now,
		ExecutorInstanceID: &executorID, ExecutorAddress: &address, ResultData: model.TaskData(`{"completed":true}`),
		ResultSummary: &summary, EndReason: &reason, CreatedAt: started, UpdatedAt: now}
	if err := db.Create(&terminal).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&terminal, terminal.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&parent, parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	return parent, terminal
}

func installStopHostBatchAudit(t *testing.T, db *gorm.DB) {
	t.Helper()
	// The schema-local statement trigger records actual committed batch sizes
	// and transaction identities, rather than inferring batching from timestamps.
	for _, statement := range []string{
		`CREATE TABLE stop_host_batch_audit (instance_id BIGINT NOT NULL, batch_size BIGINT NOT NULL, transaction_id BIGINT NOT NULL)`,
		`CREATE FUNCTION record_stop_host_batch() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		  INSERT INTO stop_host_batch_audit (instance_id, batch_size, transaction_id)
		  SELECT instance_id, count(*), txid_current() FROM changed_tasks
		  WHERE status = 6 GROUP BY instance_id;
		  RETURN NULL;
		END; $$`,
		`CREATE TRIGGER stop_host_batch_audit AFTER UPDATE ON job_task
		 REFERENCING NEW TABLE AS changed_tasks FOR EACH STATEMENT EXECUTE FUNCTION record_stop_host_batch()`,
	} {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func assertStopHostTasksAndBatches(t *testing.T, db *gorm.DB, instanceID int64, terminal model.JobTask) {
	t.Helper()
	var tasks []model.JobTask
	if err := db.Where("instance_id = ?", instanceID).Order("id").Find(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 6 {
		t.Fatalf("stopping changed the number of child rows: %d", len(tasks))
	}
	for _, task := range tasks {
		if task.ID == terminal.ID {
			if !reflect.DeepEqual(task, terminal) {
				t.Fatal("stopping overwrote an already terminal child")
			}
			continue
		}
		if task.Status != model.TaskCanceled || task.EndReason == nil || *task.EndReason != model.EndReasonManualStop ||
			task.FinishedAt == nil || task.NextDispatchAt != nil || task.AttemptNo != 1 || task.UsedAttempts != 1 ||
			task.ExecutorAddress == nil || task.ExecutorInstanceID == nil {
			t.Fatal("automatic child cancellation lost attempt, binding or terminal facts")
		}
	}
	var audit struct {
		Batches      int64
		Transactions int64
		Total        int64
		Maximum      int64
	}
	if err := db.Raw(`SELECT count(*) AS batches, count(DISTINCT transaction_id) AS transactions,
		COALESCE(sum(batch_size), 0) AS total, COALESCE(max(batch_size), 0) AS maximum
		FROM stop_host_batch_audit WHERE instance_id = ?`, instanceID).Scan(&audit).Error; err != nil {
		t.Fatal(err)
	}
	if audit.Batches != 3 || audit.Transactions != 3 || audit.Total != 5 || audit.Maximum != 2 {
		t.Fatalf("configured batches were not independently committed: %+v", audit)
	}
}

type stopHostCall struct {
	instanceID    int64
	reason        protocol.CancelReason
	authenticated bool
}

// stopHostProtocolServer stands in for a remote executor protocol endpoint. It
// checks real group credentials and controls cancellation replies; it does not
// execute or certify exit of any business handler.
type stopHostProtocolServer struct {
	protocol.UnimplementedExecutorServer
	auth     *jobexecutor.Authenticator
	response string
	entered  chan struct{}
	release  chan struct{}
	active   atomic.Int32
	mu       sync.Mutex
	calls    []stopHostCall
}

func (s *stopHostProtocolServer) CancelInstance(ctx context.Context, request *protocol.CancelInstanceRequest) (*protocol.CancelInstanceResponse, error) {
	err := s.auth.Authenticate(request.GetAuth().GetGroupName(), request.GetAuth().GetToken())
	s.mu.Lock()
	s.calls = append(s.calls, stopHostCall{instanceID: request.GetInstanceId(), reason: request.GetReason(), authenticated: err == nil})
	s.mu.Unlock()
	s.active.Add(1)
	defer s.active.Add(-1)
	s.entered <- struct{}{}
	if err != nil {
		return nil, err
	}
	switch s.response {
	case "permanent refusal":
		return nil, status.Error(codes.PermissionDenied, "synthetic cancellation refusal")
	case "blocked unavailable":
		// Keep the remote handler alive even if its caller has timed out. This
		// must not hold the scheduler's independent database cancellation path.
		<-s.release
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, "synthetic cancellation outage")
	default:
		return &protocol.CancelInstanceResponse{}, nil
	}
}

func (s *stopHostProtocolServer) snapshot() []stopHostCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]stopHostCall(nil), s.calls...)
}
