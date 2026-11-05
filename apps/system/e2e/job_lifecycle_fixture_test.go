//go:build integration && e2e

package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	testutil "github.com/tokyolab/dogx/pkg/testutil/postgres"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// jobLifecycleFixture owns one isolated schema and discovery namespace. Tests
// control only the System business fixture; Job data is accessed through public
// RPCs and the domain's real migration CLI, including after process replacement.
type jobLifecycleFixture struct {
	sqlDB                 *sql.DB
	discovery             *clientv3.Client
	endpoint, prefix, dir string
	environment           []string
	group                 *job.ExecutorGroupInfo
	processes             []*runningProcess
	ports                 map[int]bool
}

type jobLifecycleNode struct {
	process                    *runningProcess
	client                     job.JobClient
	managementPort, resultPort int
	appName                    string
}

type jobLifecycleSystem struct {
	process                      *runningProcess
	managementPort, executorPort int
	appName                      string
}

func newJobLifecycleFixture(t testing.TB, endpoints ...string) *jobLifecycleFixture {
	t.Helper()
	if len(endpoints) > 1 || len(endpoints) == 1 && strings.TrimSpace(endpoints[0]) == "" {
		t.Fatal("an externally owned test endpoint must be nonempty and supplied at most once")
	}
	var endpoint string
	if len(endpoints) == 1 {
		endpoint = endpoints[0]
	} else {
		endpoint = systemE2EEtcd(t)
	}
	_, sqlDB := testutil.OpenPostgres(t)
	applyMigrations(t, sqlDB)
	schema := currentSchema(t, sqlDB)
	createSystemJobBusinessTable(t, sqlDB)
	postgresConfig, err := pgx.ParseConfig(testutil.PostgresDSN(t))
	if err != nil {
		t.Fatal("parse lifecycle PostgreSQL configuration")
	}
	redisHost := strings.TrimSpace(os.Getenv("DOGX_TEST_REDIS_HOST"))
	if redisHost == "" {
		t.Fatal("DOGX_TEST_REDIS_HOST is required for lifecycle tests")
	}
	f := &jobLifecycleFixture{sqlDB: sqlDB, endpoint: endpoint, prefix: "/dogx/test/lifecycle/" + schema,
		dir: t.TempDir(), ports: make(map[int]bool)}
	f.environment = append(os.Environ(),
		"DOGX_E2E_ACCESS_SECRET="+e2eAccessSecret,
		"DOGX_E2E_POSTGRES_HOST="+postgresConfig.Host,
		"DOGX_E2E_POSTGRES_PORT="+strconv.FormatUint(uint64(postgresConfig.Port), 10),
		"DOGX_E2E_POSTGRES_USER="+postgresConfig.User,
		"DOGX_E2E_POSTGRES_PASSWORD="+postgresConfig.Password,
		"DOGX_E2E_POSTGRES_DATABASE="+postgresConfig.Database,
		"DOGX_E2E_REDIS_HOST="+redisHost, "PGOPTIONS=-c search_path="+schema)
	f.discovery, err = clientv3.New(clientv3.Config{Endpoints: []string{f.endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.discovery.Close() })
	// Registered before any business process cleanup, so every captured stream
	// is complete when credentials are checked, including shutdown diagnostics.
	t.Cleanup(func() {
		if f.group != nil {
			for _, process := range f.processes {
				if strings.Contains(process.logs.String(), f.group.Token) {
					t.Errorf("%s logs exposed the lifecycle group credential", process.name)
				}
			}
		}
	})
	file := filepath.Join(f.dir, "migration.yaml")
	writeFile(t, file, jobExecutionConfigYAML(f.port(t), f.port(t), f.endpoint, f.prefix))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, requiredBinary(t, "DOGX_E2E_JOB_MIGRATE_BINARY"), "-f", file, "up")
	command.Env = f.environment
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("run lifecycle Job migrations: %v\n%s", err, output)
	}
	return f
}

func createSystemJobBusinessTable(t testing.TB, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `CREATE TABLE system_job_e2e_records (
		task_id BIGINT NOT NULL, invocation_no BIGINT NOT NULL,
		instance_id BIGINT NOT NULL, job_id BIGINT NOT NULL,
		value TEXT NOT NULL, state TEXT NOT NULL,
		PRIMARY KEY (task_id, invocation_no))`)
	if err != nil {
		t.Fatalf("create System business fixture table: %v", err)
	}
}

func (f *jobLifecycleFixture) port(t testing.TB) int {
	t.Helper()
	for {
		port := freeTCPPort(t)
		if !f.ports[port] {
			f.ports[port] = true
			return port
		}
	}
}

func (f *jobLifecycleFixture) startJob(t testing.TB, name string, recoveryInterval ...time.Duration) *jobLifecycleNode {
	t.Helper()
	if len(recoveryInterval) > 1 || len(recoveryInterval) == 1 && recoveryInterval[0] <= 0 {
		t.Fatal("Job recovery interval must be positive and supplied at most once")
	}
	node := &jobLifecycleNode{managementPort: f.port(t), resultPort: f.port(t), appName: "job-lifecycle-" + uuid.NewString()}
	file := filepath.Join(f.dir, name+".yaml")
	config := jobExecutionConfigYAML(node.managementPort, node.resultPort, f.endpoint, f.prefix) + "\nShutdown:\n  WrapUpTime: 100ms\n  WaitTime: 10s\n"
	if len(recoveryInterval) == 1 {
		// Only the deployment scan interval changes; the production four-minute
		// eligibility rule and all persisted timestamps remain untouched.
		config += fmt.Sprintf("\nInstanceRecovery:\n  Interval: %s\n", recoveryInterval[0])
	}
	writeFile(t, file, config)
	env := append(append([]string(nil), f.environment...), "PGAPPNAME="+node.appName)
	node.process = startProcess(t, name, requiredBinary(t, "DOGX_E2E_JOB_RPC_BINARY"), env, "-f", file)
	f.processes = append(f.processes, node.process)
	conn := lifecycleConnection(t, node.managementPort)
	lifecycleWaitHealth(t, conn, node.process)
	lifecycleWaitHealth(t, lifecycleConnection(t, node.resultPort), node.process)
	node.client = job.NewJobClient(conn)
	f.waitDatabaseConnections(t, node.appName, true)
	return node
}

func (f *jobLifecycleFixture) createGroup(t testing.TB, node *jobLifecycleNode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	created, err := node.client.CreateExecutorGroup(ctx, &job.CreateExecutorGroupRequest{Name: "system-e2e", DiscoveryKey: f.prefix + "/executor"})
	if err != nil {
		t.Fatalf("create lifecycle group: %v", err)
	}
	response, err := node.client.GetExecutorGroup(ctx, &job.GetExecutorGroupRequest{Id: created.Id})
	if err != nil || response.GetGroup().GetToken() == "" {
		t.Fatal("read lifecycle group credential")
	}
	f.group = response.Group
}

func (f *jobLifecycleFixture) startSystem(t testing.TB, name string, withProcessor bool, shutdownWait ...time.Duration) *jobLifecycleSystem {
	t.Helper()
	wait := 10 * time.Second
	if len(shutdownWait) > 1 || len(shutdownWait) == 1 && shutdownWait[0] <= 100*time.Millisecond {
		t.Fatal("System shutdown wait must exceed the 100ms wrap-up and be supplied at most once")
	}
	if len(shutdownWait) == 1 {
		wait = shutdownWait[0]
	}
	host := &jobLifecycleSystem{managementPort: f.port(t), executorPort: f.port(t), appName: "system-lifecycle-" + uuid.NewString()}
	file := filepath.Join(f.dir, name+".yaml")
	config := rpcConfigYAML(host.managementPort, host.executorPort, f.endpoint, f.prefix,
		f.prefix+"/sessions", f.prefix+"/users", f.prefix+"/policy")
	if f.group != nil {
		config = strings.Replace(config, `Token: "00000000000040008000000000000001"`, `Token: "`+f.group.Token+`"`, 1)
	}
	writeFile(t, file, config+fmt.Sprintf("\nShutdown:\n  WrapUpTime: 100ms\n  WaitTime: %s\n", wait))
	env := append(append([]string(nil), f.environment...), "PGAPPNAME="+host.appName)
	if withProcessor {
		env = append(env, "DOGX_E2E_SYSTEM_CONFIG="+file)
		host.process = startProcess(t, name, requiredBinary(t, "DOGX_E2E_SYSTEM_EXECUTOR_BINARY"), env, "-test.run=^TestJobExecutorHostProcess$")
	} else {
		host.process = startProcess(t, name, requiredBinary(t, "DOGX_E2E_RPC_BINARY"), env, "-f", file)
	}
	f.processes = append(f.processes, host.process)
	waitForRPCReady(t, host.managementPort, host.process)
	lifecycleWaitHealth(t, lifecycleConnection(t, host.executorPort), host.process)
	jobE2EWaitDiscovery(t, f.discovery, f.prefix+"/executor", host.process)
	f.waitDatabaseConnections(t, host.appName, true)
	return host
}

func (f *jobLifecycleFixture) trigger(t testing.TB, node *jobLifecycleNode, mode string) (int64, int64) {
	t.Helper()
	params, err := json.Marshal(map[string]string{"mode": mode, "value": "lifecycle-" + mode})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	disabled, normal, retries, interval, skip, parallel := int32(0), int32(1), int32(0), int32(1), int32(0), int32(1)
	post, overridden := false, false
	created, err := node.client.CreateJob(ctx, &job.CreateJobRequest{Status: &disabled, Config: &job.JobInput{
		Name: "lifecycle " + mode, ExecutorGroupId: f.group.Id, ProcessorName: "system.e2e.persist", ExecutionMode: &normal,
		PostEnabled: &post, Params: string(params), CronExpr: "0 0 0 1 1 *", MisfirePolicy: &skip, BlockPolicy: &parallel,
		MaxRetries: &retries, RetryIntervalSeconds: &interval, TimeoutSeconds: 120,
	}})
	if err != nil {
		t.Fatalf("create lifecycle Job: %v", err)
	}
	triggered, err := node.client.TriggerJob(ctx, &job.TriggerJobRequest{Id: created.Id, RequestId: uuid.NewString(), ParamsOverridden: &overridden, OperatorId: 1})
	if err != nil {
		t.Fatalf("trigger lifecycle Job: %v", err)
	}
	return created.Id, triggered.InstanceId
}

func (f *jobLifecycleFixture) waitRunning(t testing.TB, node *jobLifecycleNode, instanceID int64, processes ...*runningProcess) *job.TaskListItem {
	t.Helper()
	var task *job.TaskListItem
	jobE2EEventually(t, processes, "accepted business and persisted ACK binding", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := node.client.ListInstanceTasks(ctx, &job.ListInstanceTasksRequest{InstanceId: instanceID, Page: 1, PageSize: 20})
		if err != nil {
			t.Fatalf("query lifecycle Task: %v", err)
		}
		if response.Total != 1 || len(response.Items) != 1 {
			return false
		}
		task = response.Items[0]
		return task.Status == 2 && task.AttemptNo == 1 && task.ExecutorInstanceId != nil && task.ExecutorAddress != nil && jobE2EBusinessState(t, f.sqlDB, task.Id, 1) == "running"
	})
	return task
}

func (f *jobLifecycleFixture) releaseBusiness(t testing.TB, taskID int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := f.sqlDB.ExecContext(ctx, "UPDATE system_job_e2e_records SET state = 'release' WHERE task_id = $1 AND invocation_no = 1 AND state = 'running'", taskID)
	if err != nil {
		t.Fatalf("release System business: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatal("System business was not waiting for release")
	}
}

func (f *jobLifecycleFixture) waitSuccess(t testing.TB, node *jobLifecycleNode, jobID, instanceID, taskID int64, processes ...*runningProcess) {
	t.Helper()
	jobE2EEventually(t, processes, "durable lifecycle result and parent completion", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := node.client.GetInstance(ctx, &job.GetInstanceRequest{Id: instanceID})
		if err != nil {
			t.Fatalf("query lifecycle Instance: %v", err)
		}
		if response.Instance.Status == 4 || response.Instance.Status == 5 {
			t.Fatalf("lifecycle Instance ended unexpectedly: status=%d reason=%s", response.Instance.Status, response.Instance.GetEndReason())
		}
		return response.Instance.Status == 3 && response.Instance.GetEndReason() == "success" && response.Instance.FinishedAt != nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := node.client.ListInstanceTasks(ctx, &job.ListInstanceTasksRequest{InstanceId: instanceID, Page: 1, PageSize: 20})
	if err != nil {
		t.Fatal(err)
	}
	if response.Total != 1 || len(response.Items) != 1 {
		t.Fatal("lifecycle completion changed the Task count")
	}
	task := response.Items[0]
	if task.Id != taskID || task.Status != 4 || task.AttemptNo != 1 || task.UsedAttempts != 1 || task.GetEndReason() != "success" || task.FinishedAt == nil {
		t.Fatal("lifecycle completion changed the original attempt or result")
	}
	if jobE2EBusinessState(t, f.sqlDB, taskID, 1) != "succeeded" {
		t.Fatal("System business completion was not persisted")
	}
	jobE2EAssertBusiness(t, f.sqlDB, taskID, instanceID, jobID, "lifecycle-gated", 1)
}

func (f *jobLifecycleFixture) waitNoDatabaseConnections(t testing.TB, appName string) {
	t.Helper()
	f.waitDatabaseConnections(t, appName, false)
}

func (f *jobLifecycleFixture) waitDatabaseConnections(t testing.TB, appName string, present bool) {
	t.Helper()
	jobE2EEventually(t, nil, "process database connection ownership", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var count int
		if err := f.sqlDB.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", appName).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return (count > 0) == present
	})
}

func lifecycleConnection(t testing.TB, port int) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(fmt.Sprintf("passthrough:///127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func lifecycleWaitHealth(t testing.TB, conn *grpc.ClientConn, process *runningProcess) {
	t.Helper()
	jobE2EEventually(t, []*runningProcess{process}, "lifecycle RPC listener ready", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		response, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		return err == nil && response.Status == grpc_health_v1.HealthCheckResponse_SERVING
	})
}

func lifecycleAssertListenerClosed(t testing.TB, port int) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("process listener %d remained open after exit", port)
	}
}
