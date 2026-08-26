package api

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type apiSystemRPCStub struct {
	systemclient.System
	request  *systemclient.ListAPIsRequest
	response *systemclient.ListAPIsResponse
	err      error
}

func (s *apiSystemRPCStub) ListAPIs(
	_ context.Context,
	request *systemclient.ListAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.ListAPIsResponse, error) {
	s.request = request
	return s.response, s.err
}

func TestListAPIsCallsSystemRPCAndMapsResponse(t *testing.T) {
	rpc := &apiSystemRPCStub{response: &systemclient.ListAPIsResponse{
		Items: []*systemclient.APIInfo{{
			Id:          11,
			ServiceName: "system-api",
			ApiGroup:    "角色管理",
			Name:        "查询角色",
			Path:        "/role/get",
			Method:      "POST",
			IsRequired:  true,
			Status:      1,
			Remark:      "built in",
		}},
	}}
	response, err := NewListAPIsLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: rpc,
	}).ListAPIs(&types.APIListReq{
		Keyword:     "role",
		ServiceName: "system-api",
		Group:       "角色管理",
	})
	if err != nil {
		t.Fatalf("list APIs: %v", err)
	}
	if rpc.request == nil || rpc.request.Keyword != "role" ||
		rpc.request.ServiceName != "system-api" || rpc.request.ApiGroup != "角色管理" {
		t.Fatalf("unexpected API list RPC request: %+v", rpc.request)
	}
	if len(response.Items) != 1 || response.Items[0].Id != 11 ||
		response.Items[0].Group != "角色管理" || !response.Items[0].IsRequired {
		t.Fatalf("unexpected API list response: %+v", response)
	}
}

func TestListAPIsRejectsInvalidAndDependencyResponses(t *testing.T) {
	if _, err := NewListAPIsLogic(context.Background(), &svc.ServiceContext{}).
		ListAPIs(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil API list request error = %v, want invalid argument", err)
	}
	if _, err := NewListAPIsLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: &apiSystemRPCStub{},
	}).ListAPIs(&types.APIListReq{}); err == nil {
		t.Fatal("empty API list RPC response was accepted")
	}
	if _, err := NewListAPIsLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: &apiSystemRPCStub{
			response: &systemclient.ListAPIsResponse{Items: []*systemclient.APIInfo{nil}},
		},
	}).ListAPIs(&types.APIListReq{}); err == nil {
		t.Fatal("nil API item was accepted")
	}
	rpcErr := errors.New("RPC unavailable")
	if _, err := NewListAPIsLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: &apiSystemRPCStub{err: rpcErr},
	}).ListAPIs(&types.APIListReq{}); !errors.Is(err, rpcErr) {
		t.Fatalf("API list error = %v, want %v", err, rpcErr)
	}
}

var _ systemclient.System = (*apiSystemRPCStub)(nil)
