//go:build integration

package execution_test

import (
	"context"
	"database/sql"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/internal/trigger"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"github.com/tokyolab/dogx/pkg/postgres"
	pgtest "github.com/tokyolab/dogx/pkg/testutil/postgres"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

func TestServiceContextDispatchesOnFollower(t *testing.T) {
	if os.Getenv("DOGX_DISPATCH_HOST_TEST") == "1" {
		testDispatchHost(t, os.Getenv("DOGX_DISPATCH_HOST_ETCD"))
		return
	}
	// The parent owns etcd even if the child hangs. zrpc's global publishers,
	// logging and shutdown hooks are confined to this one test subprocess.
	endpoint := dispatchHostEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestServiceContextDispatchesOnFollower$", "-test.v")
	command.Env = append(os.Environ(), "DOGX_DISPATCH_HOST_TEST=1", "DOGX_DISPATCH_HOST_ETCD="+endpoint)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("dispatch host subprocess: %v\n%s", err, output)
	}
	t.Log("production follower host dispatched commit hints and scanned waiting work through etcd, persisted ACK bindings, and drained its connections")
}

func testDispatchHost(t *testing.T, endpoint string) {
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatal("the parent-owned isolated etcd endpoint is required")
	}
	t.Cleanup(func() { _ = logx.Close() })
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	c, applicationName := dispatchHostConfig(t, sqlDB, endpoint)
	group := model.ExecutorGroup{Name: "host-test-workers", DiscoveryKey: c.Scheduler.Prefix + "/executor", Token: "00000000000040008000000000000001"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	job := model.JobDefinition{Name: "host dispatch", ExecutorGroupID: group.ID, ProcessorName: "test.host",
		ExecutionMode: model.ExecutionModeNormal, Params: "frozen payload", CronExpr: "0 * * * * *",
		Status: basemodel.RecordStatusDisabled, BlockPolicy: model.BlockParallel, RetryIntervalSeconds: 10, TimeoutSeconds: 300}
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.Campaign(ctx, "test-owned-leader"); err != nil {
		t.Fatal(err)
	}

	admission := &dispatchHostAdmission{calls: make(chan jobexecutor.Invocation, 4)}
	executorID := uuid.New()
	registry := jobexecutor.NewRegistry()
	if err := registry.Register(job.ProcessorName, &dispatchHostProcessor{}); err != nil {
		t.Fatal(err)
	}
	auth, err := jobexecutor.NewAuthenticator(group.Name, group.Token)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := jobexecutor.NewRPCAdapter(auth, registry, executorID.String(), admission)
	if err != nil {
		t.Fatal(err)
	}
	var serverConfig zrpc.RpcServerConf
	if err := conf.FillDefault(&serverConfig); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverConfig.Name, serverConfig.Mode = "dispatch-host-test", service.TestMode
	serverConfig.DevServer.Enabled, serverConfig.CpuThreshold = false, 0
	serverConfig.ListenOn = listener.Addr().String()
	serverConfig.Etcd = discov.EtcdConf{Hosts: []string{endpoint}, Key: group.DiscoveryKey}
	_ = listener.Close()
	server, err := jobexecutor.NewServer(serverConfig, func() *jobexecutor.RPCAdapter { return adapter })
	if err != nil {
		t.Fatal(err)
	}
	logx.SetWriter(logx.NewWriter(io.Discard))
	go server.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("stop executor RPC server: %v", err)
		}
		// Native zrpc discovery publishers live until the child exits. The
		// parent stops etcd only after that exit, not after inbound shutdown.
	})
	dispatchHostEventually(t, "leased executor discovery registration", func(ctx context.Context) bool {
		registration, err := client.Get(ctx, group.DiscoveryKey+"/", clientv3.WithPrefix())
		if err != nil {
			t.Fatal(err)
		}
		return len(registration.Kvs) == 1 && string(registration.Kvs[0].Value) == serverConfig.ListenOn && registration.Kvs[0].Lease != 0
	})

	for _, scenario := range []struct {
		name string
		hint bool
	}{
		{name: "commit hint", hint: true},
		{name: "peer scanner"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			config := c
			config.Dispatch.ScanInterval = 50 * time.Millisecond
			if scenario.hint {
				config.Dispatch.ScanInterval = time.Hour
			}
			host, err := svc.NewServiceContext(config)
			if err != nil {
				t.Fatalf("initialize production dispatch host: %v", err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = host.Drain(ctx)
				if err := host.Wait(ctx); err != nil {
					t.Errorf("wait for dispatch host cleanup: %v", err)
					return
				}
				if err := host.Close(); err != nil {
					t.Errorf("close dispatch host: %v", err)
				}
			})
			// Observe a completed empty query rather than sleeping and assuming
			// the initial scan ran. Its one-hour pause makes the commit hint the
			// only possible source of the new candidate in the first scenario.
			if scenario.hint {
				dispatchHostEventually(t, "initial empty waiting scan", func(ctx context.Context) bool {
					var done bool
					if err := sqlDB.QueryRowContext(ctx, `SELECT EXISTS (
						SELECT 1 FROM pg_stat_activity WHERE application_name = $1 AND state = 'idle'
						AND (query LIKE '%job_instance%ORDER BY id DESC%' OR query LIKE '%job_task%next_dispatch_at%'))`, applicationName).Scan(&done); err != nil {
						t.Fatal(err)
					}
					return done
				})
			}
			rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer rpcCancel()
			target, err := host.ExecutorClients.Executor(rpcCtx, group.ID)
			if err != nil {
				t.Fatalf("resolve production executor target: %v", err)
			}
			probe := &protocol.CancelInstanceRequest{Auth: &protocol.GroupAuth{GroupName: group.Name, Token: group.Token}, InstanceId: 1, Reason: protocol.CancelReason_MANUAL_STOP}
			// A no-op cancellation warms this exact borrowed connection without
			// performing a Task dispatch. Production Sender remains fail-fast.
			if _, err := target.Client.CancelInstance(rpcCtx, probe, grpc.WaitForReady(true)); err != nil {
				t.Fatalf("wait for discovered executor connection: %v", err)
			}
			creator := host.Trigger
			if !scenario.hint {
				transact, err := repository.NewTransactionRunner(db)
				if err != nil {
					t.Fatal(err)
				}
				creator, err = trigger.New(transact)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := admission.count.Load()
			instanceID, err := creator.TriggerManual(rpcCtx, trigger.ManualTrigger{JobID: job.ID, RequestID: uuid.New(), OperatorID: 42})
			if err != nil {
				t.Fatalf("create waiting Instance: %v", err)
			}
			var invocation jobexecutor.Invocation
			select {
			case invocation = <-admission.calls:
			case <-rpcCtx.Done():
				t.Fatal("production host did not dispatch the committed Instance")
			}
			if invocation.InstanceID != instanceID || invocation.JobID != job.ID || invocation.AttemptNo != 1 ||
				invocation.Stage != jobexecutor.StageOrdinary || invocation.ProcessorName != job.ProcessorName || invocation.Params != job.Params {
				t.Fatalf("executor received an unexpected invocation: %+v", invocation)
			}
			dispatchHostEventually(t, "persisted executor ACK binding", func(ctx context.Context) bool {
				var task model.JobTask
				if err := db.WithContext(ctx).First(&task, invocation.TaskID).Error; err != nil {
					t.Fatal(err)
				}
				return task.ExecutorInstanceID != nil && task.ExecutorAddress != nil
			})
			assertDispatchHostBinding(t, db, invocation, executorID, serverConfig.ListenOn)
			assertDispatchHostFollower(t, leader)

			shutdown, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			host.Stop()
			if host.Dispatch.Hint(instanceID) {
				t.Fatal("stopped host still accepted dispatch hints")
			}
			if err := host.Drain(shutdown); err != nil {
				t.Fatalf("drain dispatch host: %v", err)
			}
			if err := host.Wait(shutdown); err != nil {
				t.Fatalf("wait for dispatch host: %v", err)
			}
			if admission.count.Load() != before+1 {
				t.Fatalf("one Instance caused %d executor dispatches", admission.count.Load()-before)
			}
			if _, err := target.Client.CancelInstance(shutdown, probe); err != nil {
				t.Fatalf("drain prematurely closed the borrowed executor connection: %v", err)
			}
			if _, err := host.JobRepo.Get(shutdown, job.ID); err != nil {
				t.Fatalf("drain prematurely closed PostgreSQL: %v", err)
			}
			if err := host.NotifyJobChanged(shutdown); err != nil {
				t.Fatalf("drain prematurely closed etcd: %v", err)
			}
			if err := host.Close(); err != nil {
				t.Fatalf("close dispatch host dependencies: %v", err)
			}
			if _, err := target.Client.CancelInstance(shutdown, probe); status.Code(err) != codes.Canceled {
				t.Fatalf("borrowed executor connection was not closed: %v", err)
			}
			if _, err := host.ExecutorClients.Executor(shutdown, group.ID); err == nil {
				t.Fatal("closed host still provided executor clients")
			}
			if _, err := host.JobRepo.Get(shutdown, job.ID); err == nil {
				t.Fatal("closed host still queried PostgreSQL")
			}
			if err := host.NotifyJobChanged(shutdown); err == nil {
				t.Fatal("closed host still notified through etcd")
			}
			dispatchHostEventually(t, "released PostgreSQL pool", func(ctx context.Context) bool {
				var count int
				if err := sqlDB.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1", applicationName).Scan(&count); err != nil {
					t.Fatal(err)
				}
				return count == 0
			})
		})
	}
}

func dispatchHostConfig(t *testing.T, db *sql.DB, endpoint string) (config.Config, string) {
	t.Helper()
	parsed, err := pgx.ParseConfig(pgtest.PostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := db.QueryRow("SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(schema, "dogx_it_") {
		t.Fatal("dispatch host requires an isolated test schema")
	}
	applicationName := "dogx-dispatch-" + schema
	t.Setenv("PGOPTIONS", "-c search_path="+schema)
	t.Setenv("PGAPPNAME", applicationName)
	t.Setenv("PGCONNECT_TIMEOUT", "1")
	return config.Config{
		RpcServerConf: zrpc.RpcServerConf{Etcd: discov.EtcdConf{Hosts: []string{endpoint}, Key: "/dogx/test/job"}},
		Postgres: postgres.Config{Host: parsed.Host, Port: int(parsed.Port), User: parsed.User, Password: parsed.Password, Database: parsed.Database,
			SSLMode: "disable", TimeZone: "Asia/Shanghai", MaxIdleConns: 1, MaxOpenConns: 1, ConnMaxLifetime: time.Minute},
		Scheduler: config.SchedulerConf{Prefix: "/dogx/test/" + schema, QueueCapacity: 4, CreationConcurrency: 2,
			CreationBudget: 3 * time.Second, LeaseTTLSeconds: 15, RecoveryLimit: 4, QueryTimeout: 3 * time.Second},
		ResultPersistence:  config.ResultPersistenceConf{Capacity: 4, Concurrency: 1, SaveTimeout: time.Second},
		InstanceCheck:      config.InstanceCheckConf{Capacity: 512, Concurrency: 8, Timeout: 2 * time.Second},
		InstanceRecovery:   config.InstanceRecoveryConf{Interval: 2 * time.Minute, PageSize: 256, RunTimeout: 5 * time.Second, SQLTimeout: time.Second},
		UnboundRecovery:    config.UnboundRecoveryConf{ScanInterval: 10 * time.Second, PageSize: 128, RunTimeout: 5 * time.Second, OperationTimeout: time.Second, ObservationPeriod: 30 * time.Second},
		Stopping:           config.StoppingConf{Capacity: 256, Concurrency: 2, BatchSize: 256, MaxBatches: 4, RunTimeout: 2 * time.Second, SQLTimeout: time.Second, RecoveryPageSize: 256, RecoveryTimeout: 5 * time.Second},
		CancelNotification: config.CancelNotificationConf{InstanceCapacity: 256, PageSize: 32, TargetCapacity: 256, Concurrency: 16, QueryTimeout: time.Second, InstanceTimeout: time.Minute},
		Dispatch: config.DispatchConf{Concurrency: 2, WaitingCapacity: 4, ScanInterval: time.Second,
			TotalTimeout: 10 * time.Second, RPCTimeout: 3 * time.Second, BusyDelay: time.Second},
	}, applicationName
}

func assertDispatchHostBinding(t *testing.T, db *gorm.DB, invocation jobexecutor.Invocation, executorID uuid.UUID, address string) {
	t.Helper()
	var tasks []model.JobTask
	if err := db.Where("instance_id = ?", invocation.InstanceID).Find(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 {
		t.Fatalf("ordinary Instance has %d Tasks", len(tasks))
	}
	task := tasks[0]
	if task.ID != invocation.TaskID || task.Status != model.TaskRunning || task.TaskType != model.TaskTypeOrdinary ||
		task.AttemptNo != 1 || task.UsedAttempts != 1 || task.ExecutorInstanceID == nil || *task.ExecutorInstanceID != executorID ||
		task.ExecutorAddress == nil || *task.ExecutorAddress != address || task.StartedAt == nil || task.AttemptRegisteredAt == nil {
		t.Fatalf("unexpected persisted dispatch facts: %+v", task)
	}
	var instance model.JobInstance
	if err := db.First(&instance, invocation.InstanceID).Error; err != nil {
		t.Fatal(err)
	}
	if instance.Status != model.InstanceRunning || instance.StartedAt == nil || instance.DeadlineAt == nil || instance.FinishedAt != nil ||
		!instance.DeadlineAt.Equal(invocation.DeadlineAt) || !instance.StartedAt.Equal(*task.AttemptRegisteredAt) {
		t.Fatalf("unexpected Instance after admission: %+v", instance)
	}
}

func assertDispatchHostFollower(t *testing.T, leader *concurrency.Election) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := leader.Leader(ctx)
	if err != nil || len(response.Kvs) != 1 || string(response.Kvs[0].Value) != "test-owned-leader" {
		t.Fatalf("production host was not a follower throughout dispatch: %v", err)
	}
}

func dispatchHostEventually(t *testing.T, description string, check func(context.Context) bool) {
	t.Helper()
	dispatchHostEventuallyWithin(t, description, 5*time.Second, check)
}

func dispatchHostEventuallyWithin(t *testing.T, description string, timeout time.Duration, check func(context.Context) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if check(ctx) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}

// dispatchHostAdmission isolates first dispatch from later business execution.
// Its channel preserves the credential-free invocation received over real RPC.
type dispatchHostAdmission struct {
	calls chan jobexecutor.Invocation
	count atomic.Int32
}

func (a *dispatchHostAdmission) Accept(ctx context.Context, invocation jobexecutor.Invocation) (bool, error) {
	a.count.Add(1)
	select {
	case a.calls <- invocation:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (*dispatchHostAdmission) Cancel(context.Context, int64, jobexecutor.CancelReason) error {
	return nil
}

type dispatchHostProcessor struct{}

func (*dispatchHostProcessor) Execute(context.Context, jobexecutor.Input) (jobexecutor.Result, error) {
	panic("the admission adapter must not execute business processors")
}

func dispatchHostEtcd(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("DOGX_TEST_ETCD_BINARY")
	if binary == "" {
		t.Fatal("DOGX_TEST_ETCD_BINARY is required for the isolated etcd process")
	}
	resolved, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(resolved); err != nil || info.IsDir() {
		t.Fatalf("invalid DOGX_TEST_ETCD_BINARY: %v", err)
	}
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = clientListener.Close() }()
	peerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peerListener.Close() }()
	endpoint, peerURL := "http://"+clientListener.Addr().String(), "http://"+peerListener.Addr().String()
	dir := t.TempDir()
	log, err := os.OpenFile(filepath.Join(dir, "etcd.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	command := exec.Command(resolved, "--name", "test", "--data-dir", filepath.Join(dir, "data"),
		"--listen-client-urls", endpoint, "--advertise-client-urls", endpoint,
		"--listen-peer-urls", peerURL, "--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", "test="+peerURL, "--initial-cluster-token", filepath.Base(dir), "--log-level", "error")
	command.Stdout, command.Stderr = log, log
	_ = clientListener.Close()
	_ = peerListener.Close()
	if err := command.Start(); err != nil {
		t.Fatalf("start isolated etcd: %v", err)
	}
	finished := make(chan error, 1)
	go func() { finished <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("isolated etcd process did not exit")
		}
	})
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		probe, probeCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		response, err := client.Status(probe, endpoint)
		probeCancel()
		if err == nil && response.Leader != 0 {
			return endpoint
		}
		select {
		case <-ctx.Done():
			t.Fatal("isolated etcd did not become ready")
		case <-ticker.C:
		}
	}
}
