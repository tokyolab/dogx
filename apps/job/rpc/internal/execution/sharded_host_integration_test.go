//go:build integration

package execution_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/internal/trigger"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/execution"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"gorm.io/gorm"
)

func TestServiceContextCompletesShardedExecution(t *testing.T) {
	if os.Getenv("DOGX_SHARDED_HOST_TEST") == "1" {
		testShardedHost(t, os.Getenv("DOGX_SHARDED_HOST_ETCD"))
		return
	}
	// The parent owns etcd until the child has released process-global zrpc
	// publishers. Scheduler and executor hosts communicate over real loopback RPC.
	endpoint := dispatchHostEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestServiceContextCompletesShardedExecution$", "-test.v")
	command.Env = append(os.Environ(), "DOGX_SHARDED_HOST_TEST=1", "DOGX_SHARDED_HOST_ETCD="+endpoint)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("sharded host subprocess: %v\n%s", err, output)
	}
	t.Log("production host advanced Pre, shards, optional Post and final Instance status through real etcd discovery, RPC and PostgreSQL")
}

func testShardedHost(t *testing.T, endpoint string) {
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatal("the parent-owned isolated etcd endpoint is required")
	}
	t.Cleanup(func() { _ = logx.Close() })
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	c, _ := dispatchHostConfig(t, sqlDB, endpoint)
	c.Dispatch.ScanInterval = 50 * time.Millisecond
	c.Dispatch.WaitingCapacity, c.Dispatch.Concurrency = 16, 4
	c.ResultPersistence.Capacity = 16
	c.Postgres.MaxOpenConns, c.Postgres.MaxIdleConns = 4, 4
	c.InstanceRecovery.Interval, c.UnboundRecovery.ScanInterval = time.Hour, time.Hour
	group := model.ExecutorGroup{Name: "sharded-host-workers", DiscoveryKey: c.Scheduler.Prefix + "/executor",
		Token: "00000000000040008000000000000001"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	host, err := svc.NewServiceContext(c)
	if err != nil {
		t.Fatalf("initialize production sharded host: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
		defer cancel()
		_ = host.Drain(ctx)
		if err := host.Wait(ctx); err != nil {
			t.Errorf("wait for sharded host: %v", err)
			return
		}
		if err := host.Close(); err != nil {
			t.Errorf("close sharded host: %v", err)
		}
	})
	resultConfig := shardedHostServerConfig(t, endpoint, c.Scheduler.Prefix+"/results")
	resultServer, err := execution.NewServer(resultConfig, func() *execution.Receiver { return host.ResultReceiver })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := resultServer.Shutdown(ctx); err != nil {
			t.Errorf("stop sharded result server: %v", err)
		}
	})
	var reportConfig zrpc.RpcClientConf
	if err := conf.FillDefault(&reportConfig); err != nil {
		t.Fatal(err)
	}
	reportConfig.Etcd = resultConfig.Etcd
	reports, err := jobexecutor.NewReportClient(reportConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reports.Conn().Close() })
	processor := &shardedHostProcessor{calls: make(map[int64]shardedHostCalls)}
	registry := jobexecutor.NewRegistry()
	if err := registry.RegisterSharding("test.sharded-host", processor); err != nil {
		t.Fatal(err)
	}
	executorID := uuid.New().String()
	runtime, err := jobexecutor.NewRuntime(registry, protocol.NewResultReceiverClient(reports.Conn()), group.Name, group.Token, executorID,
		jobexecutor.RuntimeConfig{Concurrency: 4, QueueCapacity: 8, ReportCapacity: 16, ReportConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Drain(ctx); err != nil {
			t.Errorf("drain sharded executor: %v", err)
		}
	})
	auth, err := jobexecutor.NewAuthenticator(group.Name, group.Token)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := jobexecutor.NewRPCAdapter(auth, registry, executorID, runtime)
	if err != nil {
		t.Fatal(err)
	}
	executorConfig := shardedHostServerConfig(t, endpoint, group.DiscoveryKey)
	executorConfig.ServiceConf = resultConfig.ServiceConf
	executorServer, err := jobexecutor.NewServer(executorConfig, func() *jobexecutor.RPCAdapter { return adapter })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := executorServer.Shutdown(ctx); err != nil {
			t.Errorf("stop sharded executor server: %v", err)
		}
	})
	logx.SetWriter(logx.NewWriter(io.Discard))
	if err := runtime.Start(); err != nil {
		t.Fatal(err)
	}
	go resultServer.Start()
	go executorServer.Start()
	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = etcd.Close() })
	for _, serverConfig := range []zrpc.RpcServerConf{resultConfig, executorConfig} {
		dispatchHostEventually(t, "leased sharded host listener registration", func(ctx context.Context) bool {
			registered, err := etcd.Get(ctx, serverConfig.Etcd.Key+"/", clientv3.WithPrefix())
			if err != nil {
				t.Fatal(err)
			}
			return len(registered.Kvs) == 1 && string(registered.Kvs[0].Value) == serverConfig.ListenOn && registered.Kvs[0].Lease != 0
		})
	}
	ready, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReady()
	if _, err := grpc_health_v1.NewHealthClient(reports.Conn()).Check(ready, &grpc_health_v1.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("wait for discovered result receiver: %v", err)
	}
	target, err := host.ExecutorClients.Executor(ready, group.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Client.CancelInstance(ready, &protocol.CancelInstanceRequest{
		Auth: &protocol.GroupAuth{GroupName: group.Name, Token: group.Token}, InstanceId: 1, Reason: protocol.CancelReason_MANUAL_STOP,
	}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("wait for discovered executor: %v", err)
	}
	for _, scenario := range []shardedHostScenario{
		{name: "nonempty without Post", input: "normal", shards: 2, status: model.InstanceSucceeded},
		{name: "nonempty with Post", input: "normal", post: true, shards: 2, status: model.InstanceSucceeded},
		{name: "empty without Post", input: "empty", status: model.InstanceSucceeded},
		{name: "empty with Post", input: "empty", post: true, status: model.InstanceSucceeded},
		{name: "preparation failure", input: "pre_failure", post: true, status: model.InstanceFailed},
		{name: "all shards fail but Post runs", input: "shard_failure", post: true, shards: 2, failed: 2, status: model.InstanceFailed},
		{name: "Post failure", input: "post_failure", post: true, shards: 2, status: model.InstanceFailed},
		{name: "Post retries without rerunning earlier stages", input: "post_retry", post: true, shards: 2, retries: 1, status: model.InstanceSucceeded},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			definition := model.JobDefinition{Name: scenario.name, ExecutorGroupID: group.ID, ProcessorName: "test.sharded-host",
				ExecutionMode: model.ExecutionModeSharded, PostEnabled: scenario.post, Params: scenario.input, CronExpr: "0 * * * * *",
				Status: basemodel.RecordStatusDisabled, BlockPolicy: model.BlockParallel, MaxRetries: scenario.retries, RetryIntervalSeconds: 1, TimeoutSeconds: 60}
			if err := db.Create(&definition).Error; err != nil {
				t.Fatal(err)
			}
			requestCtx, cancelRequest := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelRequest()
			id, err := host.Trigger.TriggerManual(requestCtx, trigger.ManualTrigger{JobID: definition.ID, RequestID: uuid.New(), OperatorID: 42})
			if err != nil {
				t.Fatalf("trigger sharded Instance: %v", err)
			}
			var parent model.JobInstance
			// Only production result hints and shared dispatch discovery advance
			// this round. The test never calls a stage component or a hint directly.
			dispatchHostEventuallyWithin(t, "production sharded Instance completion", 10*time.Second, func(ctx context.Context) bool {
				if err := db.WithContext(ctx).First(&parent, id).Error; err != nil {
					t.Fatal(err)
				}
				return parent.Status == model.InstanceSucceeded || parent.Status == model.InstanceFailed || parent.Status == model.InstanceCanceled
			})
			if parent.Status != scenario.status || parent.FinishedAt == nil || parent.StartedAt == nil || parent.DeadlineAt == nil ||
				!parent.DeadlineAt.Equal(parent.StartedAt.Add(60*time.Second)) || parent.ExecutionConfig.PostEnabled != scenario.post {
				t.Fatalf("unexpected terminal parent: status=%d expected=%d", parent.Status, scenario.status)
			}
			assertShardedHostTasks(t, db, id, scenario, processor.snapshot(id))
		})
	}
}

type shardedHostScenario struct {
	name, input string
	post        bool
	shards      int
	failed      int
	retries     int32
	status      model.InstanceStatus
}

func assertShardedHostTasks(t *testing.T, db *gorm.DB, id int64, scenario shardedHostScenario, calls shardedHostCalls) {
	t.Helper()
	wantPost := scenario.post && scenario.input != "pre_failure"
	wantCount := 1 + scenario.shards
	wantPostCalls := 0
	if wantPost {
		wantCount++
		wantPostCalls = 1
		if scenario.input == "post_retry" {
			wantPostCalls++
		}
	}
	var tasks []model.JobTask
	if err := db.Where("instance_id = ?", id).Order("id").Find(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	if len(tasks) != wantCount || calls.pre != 1 || calls.shards != scenario.shards || calls.post != wantPostCalls {
		t.Fatalf("unexpected stage counts: tasks=%d pre=%d shards=%d post=%d", len(tasks), calls.pre, calls.shards, calls.post)
	}
	var prepareCount, shardCount, postCount int
	for _, task := range tasks {
		wantAttempts := int64(1)
		if task.TaskType == model.TaskTypePost && scenario.input == "post_retry" {
			wantAttempts = 2
		}
		if task.AttemptNo != wantAttempts || int64(task.UsedAttempts) != wantAttempts || task.StartedAt == nil || task.FinishedAt == nil || task.NextDispatchAt != nil {
			t.Fatalf("unfinished or repeated stage: task=%d status=%d attempt=%d", task.ID, task.Status, task.AttemptNo)
		}
		wantStatus := model.TaskSucceeded
		switch task.TaskType {
		case model.TaskTypePrepare:
			prepareCount++
			if scenario.input == "pre_failure" {
				wantStatus = model.TaskFailed
			}
		case model.TaskTypeShard:
			shardCount++
			if scenario.failed != 0 {
				wantStatus = model.TaskFailed
			}
		case model.TaskTypePost:
			postCount++
			if scenario.input == "post_failure" {
				wantStatus = model.TaskFailed
			}
			var saved struct {
				Total, Succeeded, Failed int64
			}
			if err := json.Unmarshal(task.InputData, &saved); err != nil || saved.Total != int64(scenario.shards) || saved.Succeeded != int64(scenario.shards-scenario.failed) || saved.Failed != int64(scenario.failed) {
				t.Fatalf("Post fixed statistics differ from the business shards: %+v, %v", saved, err)
			}
		default:
			t.Fatalf("unexpected stage type %d", task.TaskType)
		}
		if task.Status != wantStatus {
			t.Fatalf("unexpected stage outcome: type=%d status=%d expected=%d", task.TaskType, task.Status, wantStatus)
		}
	}
	wantPostCount := 0
	if wantPost {
		wantPostCount = 1
	}
	if prepareCount != 1 || shardCount != scenario.shards || postCount != wantPostCount {
		t.Fatalf("task types differ from observed processing: pre=%d shards=%d post=%d", prepareCount, shardCount, postCount)
	}
	if wantPost && (calls.postInput.Total != int64(scenario.shards) || calls.postInput.Succeeded != int64(scenario.shards-scenario.failed) || calls.postInput.Failed != int64(scenario.failed)) {
		t.Fatalf("Post received incorrect frozen statistics: %+v", calls.postInput)
	}
	if scenario.input == "post_retry" && calls.firstPostInput != calls.postInput {
		t.Fatal("Post retry changed its original Task identity or frozen statistics")
	}
}

type shardedHostCalls struct {
	pre, shards, post         int
	postInput, firstPostInput jobexecutor.PostInput
}

// shardedHostProcessor is test-owned business code. Its methods use only the
// public execution inputs and never query or manipulate scheduler persistence.
type shardedHostProcessor struct {
	mu    sync.Mutex
	calls map[int64]shardedHostCalls
}

func (p *shardedHostProcessor) PreProcess(_ context.Context, input jobexecutor.Input) (jobexecutor.PrepareResult, error) {
	p.mu.Lock()
	calls := p.calls[input.InstanceID]
	calls.pre++
	p.calls[input.InstanceID] = calls
	p.mu.Unlock()
	switch input.Params {
	case "pre_failure":
		return jobexecutor.PrepareResult{}, errors.New("synthetic preparation failure")
	case "empty":
		return jobexecutor.PrepareResult{Items: []json.RawMessage{}, Summary: "no business shards"}, nil
	default:
		return jobexecutor.PrepareResult{Items: []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`2`)}, Summary: "two business shards"}, nil
	}
}

func (p *shardedHostProcessor) ShardingProcess(_ context.Context, input jobexecutor.ShardInput) (jobexecutor.Result, error) {
	p.mu.Lock()
	calls := p.calls[input.InstanceID]
	calls.shards++
	p.calls[input.InstanceID] = calls
	p.mu.Unlock()
	if input.Params == "shard_failure" {
		return jobexecutor.Result{}, errors.New("synthetic shard failure")
	}
	return jobexecutor.Result{Data: input.Item, Summary: "business shard completed"}, nil
}

func (p *shardedHostProcessor) PostProcess(_ context.Context, input jobexecutor.PostInput) (jobexecutor.Result, error) {
	p.mu.Lock()
	calls := p.calls[input.InstanceID]
	calls.post++
	calls.postInput = input
	if calls.post == 1 {
		calls.firstPostInput = input
	}
	p.calls[input.InstanceID] = calls
	p.mu.Unlock()
	if input.Params == "post_failure" {
		return jobexecutor.Result{}, errors.New("synthetic Post failure")
	}
	if input.Params == "post_retry" && calls.post == 1 {
		return jobexecutor.Result{}, errors.New("synthetic first Post attempt failure")
	}
	return jobexecutor.Result{Summary: "Post completed"}, nil
}

func (p *shardedHostProcessor) snapshot(id int64) shardedHostCalls {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[id]
}

func shardedHostServerConfig(t *testing.T, endpoint, key string) zrpc.RpcServerConf {
	t.Helper()
	var c zrpc.RpcServerConf
	if err := conf.FillDefault(&c); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.Name, c.Mode, c.ListenOn = "sharded-host-test", service.TestMode, listener.Addr().String()
	c.DevServer.Enabled, c.CpuThreshold = false, 0
	c.Etcd = discov.EtcdConf{Hosts: []string{endpoint}, Key: key}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return c
}
