//go:build integration

package svc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/config"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	testpostgres "github.com/tokyolab/dogx/pkg/testutil/postgres"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSystemExecutorInitializationWithRealEtcdAndPostgres(t *testing.T) {
	endpoint := systemServiceEtcdEndpoint(t)
	database, sqlDB := testpostgres.OpenPostgres(t)
	if err := database.Exec("CREATE TABLE executor_business_test (task_id BIGINT PRIMARY KEY, params TEXT NOT NULL)").Error; err != nil {
		t.Fatal(err)
	}
	c := config.Config{RpcServerConf: zrpc.RpcServerConf{
		ServiceConf: service.ServiceConf{Name: "system-executor-test"}, ListenOn: "127.0.0.1:19001",
	}}
	c.JobExecutor = jobexecutor.Config{
		GroupName: "system-test", Token: systemExecutorTestToken,
		Server:       zrpc.RpcServerConf{ListenOn: "127.0.0.1:19004", Etcd: discov.EtcdConf{Hosts: []string{endpoint}, Key: "/system-test/executor"}},
		ReportClient: zrpc.RpcClientConf{Etcd: discov.EtcdConf{Hosts: []string{endpoint}, Key: "/system-test/results"}},
		Runtime:      jobexecutor.RuntimeConfig{Concurrency: 1, QueueCapacity: 2, ReportCapacity: 2, ReportConcurrency: 1},
	}
	if err := c.PrepareExecutor(); err != nil {
		t.Fatal(err)
	}
	s := &ServiceContext{Config: c, DB: database, sqlDB: sqlDB}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_ = s.Drain(ctx)
		wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := s.Wait(wait); err != nil {
			t.Errorf("executor did not actually exit: %v", err)
			return
		}
		if err := s.Close(); err != nil {
			t.Errorf("close embedded executor: %v", err)
		}
	})
	completed := make(chan error, 1)
	release := make(chan struct{})
	var registry *jobexecutor.Registry
	if err := s.initializeExecutor(func(dependencies *ServiceContext, processors *jobexecutor.Registry) error {
		registry = processors
		return processors.Register("system.test.persist", systemExecutorProcessor(func(ctx context.Context, input jobexecutor.Input) (jobexecutor.Result, error) {
			select {
			case <-release:
			case <-ctx.Done():
				completed <- ctx.Err()
				return jobexecutor.Result{}, ctx.Err()
			}
			err := dependencies.DB.WithContext(ctx).Exec("INSERT INTO executor_business_test (task_id, params) VALUES (?, ?)", input.TaskID, input.Params).Error
			completed <- err
			return jobexecutor.Result{Summary: "test business persisted"}, err
		}))
	}); err != nil {
		t.Fatalf("initialize against available etcd without an online scheduler: %v", err)
	}
	if err := registry.Register("late", systemExecutorProcessor(nil)); !errors.Is(err, jobexecutor.ErrInvalidRegistration) {
		t.Fatalf("typed nil registration error = %v", err)
	}
	if err := registry.Register("late", systemExecutorProcessor(func(context.Context, jobexecutor.Input) (jobexecutor.Result, error) {
		return jobexecutor.Result{}, nil
	})); !errors.Is(err, jobexecutor.ErrRegistryFrozen) {
		t.Fatal("executor exposed admission before freezing registration")
	}
	request := &protocol.DispatchTaskRequest{
		Auth:  &protocol.GroupAuth{GroupName: c.JobExecutor.GroupName, Token: c.JobExecutor.Token},
		JobId: 1, InstanceId: 2, TaskId: 3, AttemptNo: 1, ProcessorName: "system.test.persist", Params: "isolated business input",
		DeadlineAt: timestamppb.New(time.Now().Add(time.Minute)),
		Input:      &protocol.DispatchTaskRequest_Ordinary{Ordinary: &protocol.OrdinaryInput{}},
	}
	requestCtx, cancel := context.WithCancel(context.Background())
	response, err := s.ExecutorAdapter.DispatchTask(requestCtx, request)
	cancel()
	if err != nil || response.GetStatus() != protocol.DispatchStatus_ACCEPTED {
		t.Fatalf("dispatch into initialized executor: %v", err)
	}
	identity, err := uuid.Parse(response.GetExecutorInstanceId())
	if err != nil || identity.Version() != 4 || identity.String() != response.GetExecutorInstanceId() {
		t.Fatal("executor did not create a canonical process UUIDv4")
	}
	duplicate, err := s.ExecutorAdapter.DispatchTask(context.Background(), request)
	if err != nil || duplicate.GetExecutorInstanceId() != response.GetExecutorInstanceId() {
		t.Fatal("active duplicate changed process identity")
	}
	close(release)
	select {
	case err := <-completed:
		if err != nil {
			t.Fatalf("business did not retain live System dependencies after dispatch ended: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("business did not finish")
	}
	var count int64
	if err := database.Table("executor_business_test").Where("task_id = ? AND params = ?", request.TaskId, request.Params).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("business record count=%d err=%v", count, err)
	}
	request.TaskId++
	request.ProcessorName = "unregistered"
	if response, err := s.ExecutorAdapter.DispatchTask(context.Background(), request); err == nil || response != nil {
		t.Fatal("unknown processor was acknowledged")
	}
	s.Stop()
	request.ProcessorName = "system.test.persist"
	if response, err := s.ExecutorAdapter.DispatchTask(context.Background(), request); err == nil || response != nil {
		t.Fatal("stopping executor acknowledged new work")
	}
	budget, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	if err := s.Drain(budget); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	wait, stopWait := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopWait()
	if err := s.Wait(wait); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.reportConn.GetState() != connectivity.Shutdown {
		t.Fatal("reporting connection remained open after cleanup")
	}
}
