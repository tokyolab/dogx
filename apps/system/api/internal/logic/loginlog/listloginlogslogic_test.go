package loginlog

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type loginLogRPCStub struct {
	systemclient.System
	request  *systemclient.ListLoginLogsRequest
	response *systemclient.ListLoginLogsResponse
	err      error
}

func (s *loginLogRPCStub) ListLoginLogs(_ context.Context, req *systemclient.ListLoginLogsRequest, _ ...grpc.CallOption) (*systemclient.ListLoginLogsResponse, error) {
	s.request = req
	return s.response, s.err
}

func TestListLoginLogsMapping(t *testing.T) {
	expected := types.LoginLogItem{Id: 3, Username: "alice", Success: false, FailureReason: "invalid_credentials", IpAddress: "192.0.2.1", UserAgent: "Browser", CreatedAt: "2026-09-29T00:00:00Z"}
	rpc := &loginLogRPCStub{response: &systemclient.ListLoginLogsResponse{Total: 4, Items: []*systemclient.LoginLogInfo{{Id: expected.Id, Username: expected.Username, Success: expected.Success, FailureReason: expected.FailureReason, IpAddress: expected.IpAddress, UserAgent: expected.UserAgent, CreatedAt: expected.CreatedAt}}}}
	logic := NewListLoginLogsLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc})
	resp, err := logic.ListLoginLogs(&types.LoginLogListReq{Page: 2, PageSize: 20, Username: "alice", Result: "failure"})
	if err != nil {
		t.Fatal(err)
	}
	if rpc.request.Page != 2 || rpc.request.PageSize != 20 || rpc.request.Username != "alice" || rpc.request.Result != "failure" {
		t.Fatalf("request: %+v", rpc.request)
	}
	if resp.Total != 4 || !reflect.DeepEqual(resp.Items, []types.LoginLogItem{expected}) {
		t.Fatalf("response: %+v", resp)
	}
	rpc.response = &systemclient.ListLoginLogsResponse{}
	resp, err = logic.ListLoginLogs(&types.LoginLogListReq{Page: 1, PageSize: 20})
	if err != nil || resp.Items == nil || len(resp.Items) != 0 {
		t.Fatalf("empty response: %+v %v", resp, err)
	}
}

func TestListLoginLogsErrors(t *testing.T) {
	rpc := &loginLogRPCStub{}
	logic := NewListLoginLogsLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc})
	if _, err := logic.ListLoginLogs(nil); status.Code(err) != codes.InvalidArgument || rpc.request != nil {
		t.Fatalf("nil request: %v", err)
	}
	req := &types.LoginLogListReq{Page: 1, PageSize: 20}
	for _, failure := range []error{status.Error(codes.Unavailable, "unavailable"), status.Error(codes.InvalidArgument, "invalid")} {
		rpc.err = failure
		if _, err := logic.ListLoginLogs(req); !errors.Is(err, failure) {
			t.Fatalf("changed RPC error: %v", err)
		}
	}
	rpc.err = nil
	if _, err := logic.ListLoginLogs(req); err == nil {
		t.Fatal("nil result accepted")
	}
	rpc.response = &systemclient.ListLoginLogsResponse{Items: []*systemclient.LoginLogInfo{nil}}
	if _, err := logic.ListLoginLogs(req); err == nil {
		t.Fatal("nil item accepted")
	}
}
