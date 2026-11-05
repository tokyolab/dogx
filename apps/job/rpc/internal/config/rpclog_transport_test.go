package config

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Each subprocess owns go-zero's global logging and process hooks. The server
// barrier waits for gRPC and handlers, not Unix Start's process-wide shutdown wait.
func TestGroupDetailServerTransportLogs(t *testing.T) {
	if scenario := os.Getenv("DOGX_GROUP_SERVER_LOG_TEST"); scenario != "" {
		testGroupDetailServerTransportLogs(t, scenario)
		return
	}
	for _, scenario := range []string{"normal", "slow", "failed"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGroupDetailServerTransportLogs$", "-test.v")
			cmd.Env = append(os.Environ(), "DOGX_GROUP_SERVER_LOG_TEST="+scenario)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("server logging subprocess: %v\n%s", err, output)
			}
		})
	}
}

func testGroupDetailServerTransportLogs(t *testing.T, scenario string) {
	var c zrpc.RpcServerConf
	if err := conf.FillDefault(&c); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.ListenOn = listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	c.Name, c.Mode = "job-group-server-log-test", service.TestMode
	// Only the loopback RPC listener belongs to this test. go-zero otherwise
	// starts its default development HTTP server on every network interface.
	c.DevServer.Enabled = false
	c.CpuThreshold = 0
	c.Middlewares.StatConf.SlowThreshold = time.Hour
	if scenario != "normal" {
		// Exercise the actual slow-call path without introducing a timing race
		// between handler latency and the test machine's scheduler.
		c.Middlewares.StatConf.SlowThreshold = time.Nanosecond
	}
	ProtectServerContent(&c)
	server, err := rpchost.NewServer(c, func(s *grpc.Server) {
		job.RegisterJobServer(s, &groupLogTestServer{fail: scenario == "failed", delay: scenario != "normal"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var logs groupServerLogBuffer
	logx.SetWriter(logx.NewWriter(&logs))
	go server.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("stop group logging server: %v", err)
		}
		if err := logx.Close(); err != nil {
			t.Errorf("close group server logs: %v", err)
		}
	})
	// A plain gRPC client isolates server middleware from client-side logging.
	conn, err := grpc.NewClient(c.ListenOn, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := job.NewJobClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := client.GetExecutorGroup(ctx, &job.GetExecutorGroupRequest{Id: 7291048836217}, grpc.WaitForReady(true))
	if scenario == "failed" {
		if status.Code(err) != codes.Internal {
			t.Fatalf("group detail failure code=%v", status.Code(err))
		}
	} else if err != nil || response.GetGroup().GetToken() != groupServerLogToken {
		t.Fatalf("group detail did not deliver its credential: %v", err)
	}
	instance, instanceErr := client.GetInstance(ctx, &job.GetInstanceRequest{Id: 7291048836217})
	tasks, taskErr := client.ListInstanceTasks(ctx, &job.ListInstanceTasksRequest{InstanceId: 7291048836217, Page: 1, PageSize: 20})
	if scenario == "failed" {
		if status.Code(instanceErr) != codes.Internal || status.Code(taskErr) != codes.Internal {
			t.Fatal("execution queries lost their server failure classification")
		}
	} else if instanceErr != nil || taskErr != nil || instance.GetInstance().GetExecutionConfig().GetParams() != "execution-server-body-must-not-appear" || len(tasks.GetItems()) != 1 {
		t.Fatal("execution query transport did not return the protected data")
	}
	if scenario == "normal" {
		// The ignored normal method has no per-call log in go-zero. An unprotected
		// list call proves that this result was not achieved by disabling Stat.
		if _, err := client.ListExecutorGroups(ctx, &job.ListExecutorGroupsRequest{Page: 99112233, PageSize: 1}); err != nil {
			t.Fatal(err)
		}
	}
	output := logs.String()
	for _, content := range []string{groupServerLogToken, "group-server-body-must-not-appear", "execution-server-body-must-not-appear", "7291048836217"} {
		if strings.Contains(output, content) {
			t.Fatal("group detail content appeared in server logs")
		}
	}
	if scenario == "normal" {
		if !strings.Contains(output, job.Job_ListExecutorGroups_FullMethodName) || !strings.Contains(output, "99112233") {
			t.Fatal("normal server statistics logger was disabled")
		}
	} else if !strings.Contains(output, "slowcall") || !strings.Contains(output, job.Job_GetExecutorGroup_FullMethodName) || !strings.Contains(output, "duration") {
		t.Fatal("slow group detail call lost method or duration metadata")
	}
	if scenario != "normal" && (!strings.Contains(output, job.Job_GetInstance_FullMethodName) || !strings.Contains(output, job.Job_ListInstanceTasks_FullMethodName)) {
		t.Fatal("execution query content protection disabled method statistics")
	}
}

const groupServerLogToken = "c1ea2cc8886649369af66356211beed8"

type groupLogTestServer struct {
	job.UnimplementedJobServer
	fail  bool
	delay bool
}

func (s *groupLogTestServer) GetExecutorGroup(ctx context.Context, req *job.GetExecutorGroupRequest) (*job.GetExecutorGroupResponse, error) {
	if s.delay {
		// go-zero's stat clock uses wall time. Give it a nonzero elapsed interval
		// on hosts whose wall-clock resolution exceeds the empty handler's cost.
		timer := time.NewTimer(5 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if s.fail {
		return nil, status.Error(codes.Internal, "synthetic group lookup failure")
	}
	return &job.GetExecutorGroupResponse{Group: &job.ExecutorGroupInfo{
		Id: req.Id, Name: "group-server-body-must-not-appear", Token: groupServerLogToken,
	}}, nil
}

func (*groupLogTestServer) ListExecutorGroups(context.Context, *job.ListExecutorGroupsRequest) (*job.ListExecutorGroupsResponse, error) {
	return &job.ListExecutorGroupsResponse{}, nil
}

func (s *groupLogTestServer) GetInstance(ctx context.Context, req *job.GetInstanceRequest) (*job.GetInstanceResponse, error) {
	if _, err := s.GetExecutorGroup(ctx, &job.GetExecutorGroupRequest{Id: req.Id}); err != nil {
		return nil, err
	}
	return &job.GetInstanceResponse{Instance: &job.InstanceInfo{Id: req.Id, ExecutionConfig: &job.ExecutionConfigInfo{Params: "execution-server-body-must-not-appear"}}}, nil
}

func (s *groupLogTestServer) ListInstanceTasks(ctx context.Context, req *job.ListInstanceTasksRequest) (*job.ListInstanceTasksResponse, error) {
	if _, err := s.GetExecutorGroup(ctx, &job.GetExecutorGroupRequest{Id: req.InstanceId}); err != nil {
		return nil, err
	}
	summary := "execution-server-body-must-not-appear"
	return &job.ListInstanceTasksResponse{Items: []*job.TaskListItem{{Id: 7, InstanceId: req.InstanceId, ResultSummary: &summary}}, Total: 1}, nil
}

type groupServerLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *groupServerLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *groupServerLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
