package jobrpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type connectionStub struct {
	closed      atomic.Int32
	invoked     atomic.Int32
	method      string
	mu          sync.Mutex
	closeSignal chan struct{}
}

func (s *connectionStub) Invoke(_ context.Context, method string, _, _ any, _ ...grpc.CallOption) error {
	s.invoked.Add(1)
	s.mu.Lock()
	s.method = method
	s.mu.Unlock()
	return nil
}
func (s *connectionStub) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "not a streaming API")
}
func (s *connectionStub) Close() error {
	if s.closed.Add(1) == 1 && s.closeSignal != nil {
		close(s.closeSignal)
	}
	return nil
}

func TestNewDoesNotDiscoverOrDial(t *testing.T) {
	// Unreachable etcd must not even start discovery when constructing the BFF.
	c := New(zrpc.RpcClientConf{Etcd: discov.EtcdConf{Hosts: []string{"127.0.0.1:1"}, Key: "/test/job"}})
	if c.connection.pending != nil || c.connection.conn != nil {
		t.Fatal("New initialized discovery")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetJob(context.Background(), &job.GetJobRequest{Id: 1}); status.Code(err) != codes.Unavailable {
		t.Fatalf("closed client: %v", err)
	}
}

func TestSingleInitializationSurvivesCallerCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	stub := &connectionStub{}
	c := newClient(func() (connection, error) { calls.Add(1); close(entered); <-release; return stub, nil })
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := c.GetJob(ctx, &job.GetJobRequest{Id: 1}); result <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initializer not started")
	}
	cancel()
	if err := <-result; status.Code(err) != codes.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
	c.connection.mu.Lock()
	pending := c.connection.pending
	c.connection.mu.Unlock()
	var wg sync.WaitGroup
	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := c.GetJob(ctx, &job.GetJobRequest{}); status.Code(err) != codes.Canceled {
				t.Errorf("canceled caller: %v", err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("initializations=%d", calls.Load())
	}
	close(release)
	select {
	case <-pending.done:
	case <-time.After(time.Second):
		t.Fatal("initialization did not finish")
	}
	if _, err := c.GetJob(context.Background(), &job.GetJobRequest{Id: 2}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || stub.invoked.Load() != 1 {
		t.Fatal("client did not reuse initialized connection")
	}
}

func TestInitializationHasRequestBudgetAndLateClose(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	stub := &connectionStub{closeSignal: make(chan struct{})}
	c := newClient(func() (connection, error) { close(entered); <-release; return stub, nil })
	c.connection.timeout = 10 * time.Millisecond
	result := make(chan error, 1)
	go func() { _, err := c.GetJob(context.Background(), &job.GetJobRequest{}); result <- err }()
	<-entered
	if err := <-result; status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("initialization budget: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case <-stub.closeSignal:
	case <-time.After(time.Second):
		t.Fatal("late connection leaked")
	}
	if stub.closed.Load() != 1 {
		t.Fatal("connection closed more than once")
	}
}

func TestInitializationFailureCanRetryWithoutExposingDetails(t *testing.T) {
	var calls atomic.Int32
	stub := &connectionStub{}
	c := newClient(func() (connection, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("postgres://secret@example")
		}
		return stub, nil
	})
	t.Cleanup(func() { _ = c.Close() })
	if _, err := c.GetJob(context.Background(), &job.GetJobRequest{}); status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "job service is unavailable" {
		t.Fatalf("failure: %v", err)
	}
	if _, err := c.GetJob(context.Background(), &job.GetJobRequest{}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if stub.closed.Load() != 1 {
		t.Fatal("ready connection leaked")
	}
}

func TestSensitiveJobMethodsDoNotLogContent(t *testing.T) {
	methods := map[string]bool{}
	protectContent(func(method string) { methods[method] = true })
	for _, method := range []string{job.Job_CreateJob_FullMethodName, job.Job_UpdateJob_FullMethodName, job.Job_GetJob_FullMethodName, job.Job_TriggerJob_FullMethodName} {
		if !methods[method] {
			t.Fatalf("missing protection: %s", method)
		}
	}
}

func TestStreamFailureAndForwarding(t *testing.T) {
	c := newClient(func() (connection, error) { return &connectionStub{}, nil })
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.connection.NewStream(ctx, &grpc.StreamDesc{}, "/test"); status.Code(err) != codes.Canceled {
		t.Fatalf("stream cancellation: %v", err)
	}
	if _, err := c.connection.NewStream(context.Background(), &grpc.StreamDesc{}, "/test"); status.Code(err) != codes.Unimplemented {
		t.Fatalf("stream forwarding: %v", err)
	}
}
