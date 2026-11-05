package svc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	systemExecutorTestToken = "11111111111141118111111111111111"
	systemExecutorTestID    = "22222222-2222-4222-8222-222222222222"
)

type systemExecutorProcessor func(context.Context, jobexecutor.Input) (jobexecutor.Result, error)

func (f systemExecutorProcessor) Execute(ctx context.Context, input jobexecutor.Input) (jobexecutor.Result, error) {
	return f(ctx, input)
}

type systemExecutorReportClient struct {
	call func(context.Context) error
}

func (c *systemExecutorReportClient) ReportTaskResult(ctx context.Context, _ *protocol.ReportTaskResultRequest, _ ...grpc.CallOption) (*protocol.ReportTaskResultResponse, error) {
	if c.call != nil {
		if err := c.call(ctx); err != nil {
			return nil, err
		}
	}
	return &protocol.ReportTaskResultResponse{}, nil
}

// lifecycleConnector provides a real sql.DB lifecycle without network access.
// Its connections expose early closure while business or reporting still runs.
type lifecycleConnector struct{ closed atomic.Bool }

func (c *lifecycleConnector) Connect(context.Context) (driver.Conn, error) {
	return &lifecycleConnection{owner: c}, nil
}

func (c *lifecycleConnector) Driver() driver.Driver { return lifecycleDriver{} }

type lifecycleDriver struct{}

func (lifecycleDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("lifecycle test uses its connector")
}

type lifecycleConnection struct{ owner *lifecycleConnector }

func (c *lifecycleConnection) Close() error {
	c.owner.closed.Store(true)
	return nil
}

func (*lifecycleConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not used by this lifecycle test")
}

func (*lifecycleConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("statements are not used by this lifecycle test")
}

func (*lifecycleConnection) Ping(context.Context) error { return nil }

func TestServiceContextCloseWaitsForActualExecutorCompletion(t *testing.T) {
	for _, blocked := range []string{"business", "report"} {
		t.Run(blocked, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan context.Context, 1)
				release := make(chan struct{})
				connector := &lifecycleConnector{}
				database := sql.OpenDB(connector)
				if err := database.PingContext(context.Background()); err != nil {
					t.Fatal(err)
				}
				connection, err := grpc.NewClient("passthrough:///127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				registry := jobexecutor.NewRegistry()
				if err := registry.Register("lifecycle.test", systemExecutorProcessor(func(ctx context.Context, _ jobexecutor.Input) (jobexecutor.Result, error) {
					if blocked == "business" {
						entered <- ctx
						<-release
					}
					return jobexecutor.Result{}, nil
				})); err != nil {
					t.Fatal(err)
				}
				reporter := &systemExecutorReportClient{call: func(ctx context.Context) error {
					if blocked == "report" {
						entered <- ctx
						<-release
					}
					return nil
				}}
				runtime, err := jobexecutor.NewRuntime(registry, reporter, "system-test", systemExecutorTestToken, systemExecutorTestID,
					jobexecutor.RuntimeConfig{Concurrency: 1, QueueCapacity: 1, ReportCapacity: 1, ReportConcurrency: 1})
				if err != nil {
					t.Fatal(err)
				}
				s := &ServiceContext{Executor: runtime, sqlDB: database, reportConn: connection}
				if err := runtime.Start(); err != nil {
					t.Fatal(err)
				}
				accepted, err := runtime.Accept(context.Background(), jobexecutor.Invocation{
					Input: jobexecutor.Input{JobID: 1, InstanceID: 2, TaskID: 3}, AttemptNo: 1,
					ProcessorName: "lifecycle.test", DeadlineAt: time.Now().Add(time.Hour), Stage: jobexecutor.StageOrdinary,
				})
				if err != nil || !accepted {
					t.Fatalf("accept business work: accepted=%t err=%v", accepted, err)
				}
				workCtx := <-entered
				closed := make(chan error, 1)
				go func() { closed <- s.Close() }()
				synctest.Wait()
				budget, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := s.Drain(budget); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("shared drain budget = %v", err)
				}
				synctest.Wait()
				if workCtx.Err() == nil {
					t.Fatal("shared drain expiry did not cancel accepted work")
				}
				if connector.closed.Load() || connection.GetState() == connectivity.Shutdown {
					t.Fatal("dependencies closed before business/reporting actually returned")
				}
				select {
				case <-closed:
					t.Fatal("Close treated cancellation as actual completion")
				default:
				}
				close(release)
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
				if !connector.closed.Load() || connection.GetState() != connectivity.Shutdown {
					t.Fatal("dependencies remained open after actual completion")
				}
				if err := s.Close(); err != nil {
					t.Fatalf("repeated Close: %v", err)
				}
			})
		})
	}
}

func TestExecutorRegistrationFailureClosesInitializedDependencies(t *testing.T) {
	for _, panics := range []bool{false, true} {
		connector := &lifecycleConnector{}
		database := sql.OpenDB(connector)
		if err := database.PingContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		s := &ServiceContext{sqlDB: database}
		failure := errors.New("synthetic processor registration failure")
		var observed any
		func() {
			defer func() { observed = recover() }()
			err := s.initializeExecutor(func(current *ServiceContext, _ *jobexecutor.Registry) error {
				if current != s {
					t.Fatal("registration received another dependency container")
				}
				if panics {
					panic(failure)
				}
				return failure
			})
			if !errors.Is(err, failure) {
				t.Fatalf("registration error = %v", err)
			}
		}()
		panicErr, _ := observed.(error)
		if panics && !errors.Is(panicErr, failure) || !panics && observed != nil {
			t.Fatal("registration panic was swallowed or invented")
		}
		if !connector.closed.Load() || s.Executor != nil || s.reportConn != nil {
			t.Fatal("failed registration retained dependencies or started execution")
		}
	}
}
