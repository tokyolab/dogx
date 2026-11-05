//go:build integration && e2e

package execution_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	pgtest "github.com/tokyolab/dogx/pkg/testutil/postgres"
	testprocess "github.com/tokyolab/dogx/pkg/testutil/process"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestProcessSignalSender(t *testing.T) { testprocess.RunSignalSender(t) }

// TestJobResultAcknowledgementProcessRecoveryEndToEnd distinguishes accepting a
// result into memory from saving it. A real Task row lock blocks the sole saver
// on a first result, so a second acknowledged result can only wait in memory.
// Graceful shutdown must drain that work; killing the process loses the queued
// result and leaves the bound attempt for the ordinary Instance timeout path,
// without authorizing another business attempt.
//
// This domain-owned fixture supplies an old, expired but still RUNNING round and
// a protocol result; it does not pretend to execute business or wait four minutes.
// An external election holder prevents timeout recovery until A has exited.
func TestJobResultAcknowledgementProcessRecoveryEndToEnd(t *testing.T) {
	for _, graceful := range []bool{true, false} {
		name := "crash_loses_accepted_result"
		if graceful {
			name = "signal_drains_accepted_result"
		}
		t.Run(name, func(t *testing.T) {
			db, sqlDB := testutil.OpenMigratedPostgres(t)
			endpoint := dispatchHostEtcd(t)
			prefix := "/dogx/test/result-process/" + uuid.NewString()
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
			leader := concurrency.NewElection(session, prefix+"/scheduler/election")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := leader.Campaign(ctx, "test-owned-leader"); err != nil {
				cancel()
				t.Fatal(err)
			}
			cancel()

			// Own the address used by best-effort cancellation. This protocol-only
			// fixture deliberately has no dispatch implementation or discovery entry.
			listener := resultProcessListener(t)
			executor := grpc.NewServer()
			protocol.RegisterExecutorServer(executor, &protocol.UnimplementedExecutorServer{})
			serving := make(chan struct{})
			go func() { defer close(serving); _ = executor.Serve(listener) }()
			t.Cleanup(func() { executor.Stop(); <-serving })
			group := model.ExecutorGroup{Name: "result-process-workers", DiscoveryKey: prefix + "/executor", Token: "00000000000040008000000000000001"}
			if err := db.Create(&group).Error; err != nil {
				t.Fatal(err)
			}
			var now time.Time
			if err := db.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
				t.Fatal(err)
			}
			round := createRunningRecoveryRound(t, db, group, listener.Addr().String(), name, model.ExecutionModeNormal,
				now.Add(-10*time.Minute), now.Add(-5*time.Minute), model.TaskRunning)
			blockingRound := createRunningRecoveryRound(t, db, group, listener.Addr().String(), name+" blocker", model.ExecutionModeNormal,
				now.Add(-10*time.Minute), now.Add(-5*time.Minute), model.TaskRunning)
			nodeA := startResultProcess(t, sqlDB, endpoint, prefix, group.Token)
			assertDispatchHostFollower(t, leader)

			lockCtx, lockCancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer lockCancel()
			locked, err := sqlDB.BeginTx(lockCtx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = locked.Rollback() })
			var blockerPID int
			if err := locked.QueryRowContext(lockCtx, "SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
				t.Fatal(err)
			}
			var blockingTaskID int64
			taskID := round.task.ID
			// This single-primary-key transaction prevents the real result UPDATE
			// from committing. Holding it across RPC/exit is intentional fault
			// injection in this test, not a production transaction pattern.
			if err := locked.QueryRowContext(lockCtx, "SELECT id FROM job_task WHERE id = $1 FOR UPDATE", blockingRound.task.ID).Scan(&blockingTaskID); err != nil {
				t.Fatal(err)
			}
			report := func(target runningRecoveryRound) {
				t.Helper()
				reportCtx, reportCancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer reportCancel()
				_, err := protocol.NewResultReceiverClient(nodeA.results).ReportTaskResult(reportCtx, &protocol.ReportTaskResultRequest{
					Auth:  &protocol.GroupAuth{GroupName: group.Name, Token: group.Token},
					JobId: target.parent.JobID, InstanceId: target.parent.ID, TaskId: target.task.ID, AttemptNo: 1,
					ExecutorInstanceId: target.task.ExecutorInstanceID.String(), Outcome: protocol.TaskOutcome_SUCCESS,
					Summary: "Accepted result survived graceful shutdown.",
					Output:  &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte("9007199254740993")},
				})
				if err != nil {
					t.Fatalf("receive memory-admission ACK: %v", err)
				}
			}
			report(blockingRound)
			isBlocked := func(ctx context.Context) bool {
				nodeA.assertAlive(t)
				var blocked bool
				err := sqlDB.QueryRowContext(ctx, `SELECT EXISTS (
					SELECT 1 FROM pg_stat_activity WHERE application_name = $1
					AND $2 = ANY(pg_blocking_pids(pid)) AND query LIKE '%UPDATE%job_task%')`, nodeA.appName, blockerPID).Scan(&blocked)
				if err != nil {
					t.Fatal(err)
				}
				return blocked
			}
			dispatchHostEventually(t, "acknowledged saver blocked by the exact test transaction", isBlocked)
			// The sole saver cannot start this result while the first UPDATE is
			// blocked. Its acknowledgement therefore proves memory admission only.
			report(round)
			assertQueued := func() {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if !isBlocked(ctx) {
					t.Fatal("the only saver stopped blocking before the fault was applied")
				}
				assertRunningRecoveryUnchanged(t, db, round)
			}
			assertQueued()
			if graceful {
				testprocess.Signal(t, nodeA.cmd, nodeA.done)
				for _, connection := range []*grpc.ClientConn{nodeA.management, nodeA.results} {
					dispatchHostEventually(t, "stopped result-process RPC admission while its saver drains", func(ctx context.Context) bool {
						nodeA.assertAlive(t)
						probe, stop := context.WithTimeout(ctx, 200*time.Millisecond)
						defer stop()
						_, err := grpc_health_v1.NewHealthClient(connection).Check(probe, &grpc_health_v1.HealthCheckRequest{})
						return status.Code(err) == codes.Unavailable
					})
				}
				assertQueued()
				if err := locked.Rollback(); err != nil {
					t.Fatal(err)
				}
				if err := nodeA.wait(t, 15*time.Second); err != nil {
					t.Fatalf("graceful result process exit: %v\n%s", err, nodeA.log(t))
				}
			} else {
				if err := nodeA.cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				if err := nodeA.wait(t, 5*time.Second); err == nil {
					t.Fatal("killed result process reported clean exit")
				}
				if err := locked.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			nodeA.waitNoConnections(t, sqlDB)
			if !graceful {
				// The first result was in flight when A died; its outcome is not
				// inferred from the lost connection. Only the queued target is
				// asserted here, after every old database session has gone away.
				assertRunningRecoveryUnchanged(t, db, round)
			}
			var saved model.JobTask
			if err := db.First(&saved, taskID).Error; err != nil {
				t.Fatal(err)
			}
			if graceful && (saved.Status != model.TaskSucceeded || string(saved.ResultData) != "9007199254740993" || saved.FinishedAt == nil) {
				t.Fatal("normal process exit did not drain the acknowledged result")
			}

			// Keep B a follower until its dependencies are ready. Releasing only
			// the external election lets B run its ordinary initial leader scan;
			// no direct checker call, hint or timestamp rewrite supplies progress.
			nodeB := startResultProcess(t, sqlDB, endpoint, prefix, group.Token)
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			err = leader.Resign(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			wantParent, wantTask, wantReason := model.InstanceFailed, model.TaskCanceled, model.EndReasonInstanceTimeout
			if graceful {
				wantParent, wantTask, wantReason = model.InstanceSucceeded, model.TaskSucceeded, model.EndReasonSuccess
			}
			var parent model.JobInstance
			// Include natural expiry of A's abandoned election lease after a crash.
			dispatchHostEventuallyWithin(t, "replacement leader converged the original round", 30*time.Second, func(ctx context.Context) bool {
				nodeB.assertAlive(t)
				if err := db.WithContext(ctx).First(&parent, round.parent.ID).Error; err != nil {
					t.Fatal(err)
				}
				return parent.Status == wantParent
			})
			var tasks []model.JobTask
			if err := db.Where("instance_id = ?", parent.ID).Find(&tasks).Error; err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 1 {
				t.Fatalf("recovery changed ordinary Task count to %d", len(tasks))
			}
			saved = tasks[0]
			if parent.FinishedAt == nil || parent.EndReason == nil || *parent.EndReason != wantReason ||
				saved.ID != taskID || saved.Status != wantTask || saved.EndReason == nil || *saved.EndReason != wantReason || saved.FinishedAt == nil ||
				saved.AttemptNo != 1 || saved.UsedAttempts != 1 || saved.NextDispatchAt != nil ||
				saved.ExecutorInstanceID == nil || *saved.ExecutorInstanceID != *round.task.ExecutorInstanceID ||
				saved.ExecutorAddress == nil || *saved.ExecutorAddress != *round.task.ExecutorAddress {
				t.Fatal("replacement recovery changed attempt identity, binding or terminal facts")
			}
			if graceful && string(saved.ResultData) != "9007199254740993" || !graceful && saved.ResultData != nil {
				t.Fatal("recovery invented or lost committed result data")
			}
			if !parent.StartedAt.Equal(*round.parent.StartedAt) || !parent.DeadlineAt.Equal(*round.parent.DeadlineAt) {
				t.Fatal("recovery changed the frozen round deadline")
			}
		})
	}
}

// resultProcess owns only a real Job executable and its observation handles.
// err is read only after done closes; the test owns forced cleanup on failure.
type resultProcess struct {
	cmd                 *exec.Cmd
	done                chan struct{}
	err                 error
	appName, logPath    string
	management, results *grpc.ClientConn
}

func startResultProcess(t *testing.T, db *sql.DB, endpoint, prefix, token string) *resultProcess {
	t.Helper()
	binary := os.Getenv("DOGX_E2E_JOB_RPC_BINARY")
	if info, err := os.Stat(binary); err != nil || info.IsDir() {
		t.Fatal("DOGX_E2E_JOB_RPC_BINARY must name a built production Job executable")
	}
	parsed, err := pgx.ParseConfig(pgtest.PostgresDSN(t))
	if err != nil {
		t.Fatal("parse result-process PostgreSQL configuration")
	}
	var schema string
	if err := db.QueryRow("SELECT current_schema()").Scan(&schema); err != nil || !strings.HasPrefix(schema, "dogx_it_") {
		t.Fatal("result process requires an isolated test schema")
	}
	management, results := resultProcessListener(t), resultProcessListener(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "job-rpc.yaml")
	config := fmt.Sprintf(`Name: result-process-test
ListenOn: %s
Mode: test
Timeout: 5000
DevServer:
  Enabled: false
Etcd:
  Hosts: [%q]
  Key: %s/management
ExecutionRPC:
  Name: result-process-receiver
  ListenOn: %s
  Auth: false
  Health: true
  Etcd:
    Hosts: [%q]
    Key: %s/results
Scheduler:
  Prefix: %s
ResultPersistence:
  Concurrency: 1
  SaveTimeout: 1m
Shutdown:
  WrapUpTime: 100ms
  WaitTime: 10s
Postgres:
  Host: ${DOGX_RESULT_PROCESS_PG_HOST}
  Port: ${DOGX_RESULT_PROCESS_PG_PORT}
  User: ${DOGX_RESULT_PROCESS_PG_USER}
  Password: ${DOGX_RESULT_PROCESS_PG_PASSWORD}
  Database: ${DOGX_RESULT_PROCESS_PG_DATABASE}
  SSLMode: disable
  TimeZone: UTC
  MaxIdleConns: 2
  MaxOpenConns: 10
  ConnMaxLifetime: 5m
`, management.Addr(), endpoint, prefix, results.Addr(), endpoint, prefix, prefix)
	if err := os.WriteFile(file, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	p := &resultProcess{done: make(chan struct{}), appName: "result-process-" + uuid.NewString(), logPath: filepath.Join(dir, "process.log")}
	output, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = output.Close() })
	p.cmd = exec.Command(binary, "-f", file)
	testprocess.Configure(p.cmd)
	p.cmd.Env = append(os.Environ(), "PGOPTIONS=-c search_path="+schema, "PGAPPNAME="+p.appName,
		"DOGX_RESULT_PROCESS_PG_HOST="+parsed.Host, fmt.Sprintf("DOGX_RESULT_PROCESS_PG_PORT=%d", parsed.Port),
		"DOGX_RESULT_PROCESS_PG_USER="+parsed.User, "DOGX_RESULT_PROCESS_PG_PASSWORD="+parsed.Password,
		"DOGX_RESULT_PROCESS_PG_DATABASE="+parsed.Database)
	p.cmd.Stdout, p.cmd.Stderr = output, output
	_ = management.Close()
	_ = results.Close()
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = p.cmd.Process.Kill()
			_ = p.wait(t, 5*time.Second)
		}
		if strings.Contains(p.log(t), token) {
			t.Error("Job process logs exposed its test group credential")
		}
	})
	p.management = p.connect(t, management.Addr().String())
	p.results = p.connect(t, results.Addr().String())
	dispatchHostEventually(t, "production process PostgreSQL connection ownership", func(ctx context.Context) bool {
		p.assertAlive(t)
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", p.appName).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count > 0
	})
	return p
}

func resultProcessListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func (p *resultProcess) connect(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	dispatchHostEventuallyWithin(t, "production result-process RPC readiness", 15*time.Second, func(ctx context.Context) bool {
		p.assertAlive(t)
		probe, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		response, err := grpc_health_v1.NewHealthClient(conn).Check(probe, &grpc_health_v1.HealthCheckRequest{})
		return err == nil && response.Status == grpc_health_v1.HealthCheckResponse_SERVING
	})
	return conn
}

func (p *resultProcess) wait(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(timeout):
		t.Fatal("production Job process did not exit before the observation deadline")
		return nil
	}
}

func (p *resultProcess) assertAlive(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatalf("production Job process exited unexpectedly: %v\n%s", p.err, p.log(t))
	default:
	}
}

func (p *resultProcess) log(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(p.logPath)
	if err != nil {
		t.Fatalf("read Job process diagnostic log: %v", err)
	}
	return string(data)
}

func (p *resultProcess) waitNoConnections(t *testing.T, db *sql.DB) {
	t.Helper()
	dispatchHostEventually(t, "exited Job process database connections released", func(ctx context.Context) bool {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", p.appName).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count == 0
	})
}
