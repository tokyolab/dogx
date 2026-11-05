package jobrpc

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGroupDetailClientLogsOmitCredentialsOnNormalSlowAndFailedCalls(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	job.RegisterJobServer(server, &groupLoggingServer{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() { server.Stop(); <-stopped })
	var logs groupLogBuffer
	previous := logx.Reset()
	logx.SetWriter(logx.NewWriter(&logs))
	t.Cleanup(func() { logx.SetWriter(previous) })
	var cfg zrpc.RpcClientConf
	if err := conf.FillDefault(&cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Endpoints = []string{listener.Addr().String()}
	client := New(cfg)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, id := range []int64{1, 2} {
		response, err := client.GetExecutorGroup(ctx, &job.GetExecutorGroupRequest{Id: id}, grpc.WaitForReady(true))
		if err != nil || response.GetGroup().GetToken() != groupLogToken {
			t.Fatalf("group detail call %d did not deliver its credential: %v", id, err)
		}
		instance, err := client.GetInstance(ctx, &job.GetInstanceRequest{Id: id})
		if err != nil || instance.GetInstance().GetExecutionConfig().GetParams() != "execution-response-must-not-be-logged" {
			t.Fatal("execution detail did not deliver frozen parameters")
		}
		tasks, err := client.ListInstanceTasks(ctx, &job.ListInstanceTasksRequest{InstanceId: id, Page: 1, PageSize: 20})
		if err != nil || len(tasks.GetItems()) != 1 || tasks.Items[0].GetResultSummary() != "execution-response-must-not-be-logged" {
			t.Fatal("task query did not deliver its summary")
		}
	}
	if _, err := client.GetExecutorGroup(ctx, &job.GetExecutorGroupRequest{Id: 9988776611}); status.Code(err) != codes.Internal {
		t.Fatalf("group detail error=%v", err)
	}
	if _, err := client.GetInstance(ctx, &job.GetInstanceRequest{Id: 9988776611}); status.Code(err) != codes.Internal {
		t.Fatal("instance detail failure classification changed")
	}
	if _, err := client.ListInstanceTasks(ctx, &job.ListInstanceTasksRequest{InstanceId: 9988776611, Page: 1, PageSize: 20}); status.Code(err) != codes.Internal {
		t.Fatal("task query failure classification changed")
	}
	output := logs.String()
	if !strings.Contains(output, "slowcall") || !strings.Contains(output, "fail -") || !strings.Contains(output, job.Job_GetExecutorGroup_FullMethodName) {
		t.Fatal("RPC duration/error metadata was not preserved")
	}
	for _, secret := range []string{groupLogToken, "group-response-must-not-be-logged", "execution-response-must-not-be-logged", "9988776611"} {
		if strings.Contains(output, secret) {
			t.Fatal("group RPC content appeared in client logs")
		}
	}
	if !strings.Contains(output, job.Job_GetInstance_FullMethodName) || !strings.Contains(output, job.Job_ListInstanceTasks_FullMethodName) {
		t.Fatal("execution query protection removed method statistics")
	}
}

const groupLogToken = "c1ea2cc8886649369af66356211beed8"

type groupLoggingServer struct{ job.UnimplementedJobServer }

func (*groupLoggingServer) GetExecutorGroup(ctx context.Context, req *job.GetExecutorGroupRequest) (*job.GetExecutorGroupResponse, error) {
	if req.Id == 2 {
		// Exercise the actual default 500 ms client slow-response logger rather
		// than disabling statistics or replacing the interceptor with a stub.
		timer := time.NewTimer(550 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if req.Id == 9988776611 {
		return nil, status.Error(codes.Internal, "synthetic group lookup failure")
	}
	return &job.GetExecutorGroupResponse{Group: &job.ExecutorGroupInfo{Id: req.Id, Name: "group-response-must-not-be-logged", Token: groupLogToken}}, nil
}

func (s *groupLoggingServer) GetInstance(ctx context.Context, req *job.GetInstanceRequest) (*job.GetInstanceResponse, error) {
	if _, err := s.GetExecutorGroup(ctx, &job.GetExecutorGroupRequest{Id: req.Id}); err != nil {
		return nil, err
	}
	return &job.GetInstanceResponse{Instance: &job.InstanceInfo{Id: req.Id, ExecutionConfig: &job.ExecutionConfigInfo{Params: "execution-response-must-not-be-logged"}}}, nil
}

func (s *groupLoggingServer) ListInstanceTasks(ctx context.Context, req *job.ListInstanceTasksRequest) (*job.ListInstanceTasksResponse, error) {
	if _, err := s.GetExecutorGroup(ctx, &job.GetExecutorGroupRequest{Id: req.InstanceId}); err != nil {
		return nil, err
	}
	summary := "execution-response-must-not-be-logged"
	return &job.ListInstanceTasksResponse{Items: []*job.TaskListItem{{Id: 7, InstanceId: req.InstanceId, ResultSummary: &summary}}, Total: 1}, nil
}

type groupLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *groupLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *groupLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
