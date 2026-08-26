package role

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

type roleQuerySystemRPCStub struct {
	systemclient.System
	listRequest  *systemclient.ListRolesRequest
	listResponse *systemclient.ListRolesResponse
	getRequest   *systemclient.GetRoleRequest
	getResponse  *systemclient.GetRoleResponse
	apiRequest   *systemclient.GetRoleAPIsRequest
	apiResponse  *systemclient.GetRoleAPIsResponse
	err          error
}

func (s *roleQuerySystemRPCStub) ListRoles(
	_ context.Context,
	request *systemclient.ListRolesRequest,
	_ ...grpc.CallOption,
) (*systemclient.ListRolesResponse, error) {
	s.listRequest = request
	return s.listResponse, s.err
}

func (s *roleQuerySystemRPCStub) GetRole(
	_ context.Context,
	request *systemclient.GetRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.GetRoleResponse, error) {
	s.getRequest = request
	return s.getResponse, s.err
}

func (s *roleQuerySystemRPCStub) GetRoleAPIs(
	_ context.Context,
	request *systemclient.GetRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.GetRoleAPIsResponse, error) {
	s.apiRequest = request
	return s.apiResponse, s.err
}

func TestListRolesCallsSystemRPCAndMapsResponse(t *testing.T) {
	rpc := &roleQuerySystemRPCStub{listResponse: &systemclient.ListRolesResponse{
		Items: []*systemclient.RoleInfo{{
			Id:          7,
			Code:        "operator",
			Name:        "Operator",
			Description: "Operations role",
			Sort:        3,
			Status:      1,
			IsSystem:    true,
			CreatedAt:   "2026-08-26T01:02:03Z",
			UpdatedAt:   "2026-08-26T01:03:03Z",
		}},
		Total: 2,
	}}
	response, err := NewListRolesLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: rpc,
	}).ListRoles(&types.RoleListReq{Page: 2, PageSize: 20, Keyword: "operator"})
	if err != nil {
		t.Fatalf("list roles: %v", err)
	}
	if rpc.listRequest == nil || rpc.listRequest.Page != 2 ||
		rpc.listRequest.PageSize != 20 || rpc.listRequest.Keyword != "operator" {
		t.Fatalf("unexpected role list RPC request: %+v", rpc.listRequest)
	}
	if response.Total != 2 || len(response.Items) != 1 ||
		response.Items[0].Id != 7 || response.Items[0].Sort != 3 || !response.Items[0].IsSystem {
		t.Fatalf("unexpected role list response: %+v", response)
	}
}

func TestListRolesRejectsInvalidOrEmptyResponses(t *testing.T) {
	if _, err := NewListRolesLogic(context.Background(), &svc.ServiceContext{}).
		ListRoles(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil role list request error = %v, want invalid argument", err)
	}
	if _, err := NewListRolesLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: &roleQuerySystemRPCStub{},
	}).ListRoles(&types.RoleListReq{}); err == nil {
		t.Fatal("empty role list RPC response was accepted")
	}
	if _, err := NewListRolesLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: &roleQuerySystemRPCStub{
			listResponse: &systemclient.ListRolesResponse{Items: []*systemclient.RoleInfo{nil}},
		},
	}).ListRoles(&types.RoleListReq{}); err == nil {
		t.Fatal("nil role item was accepted")
	}
}

func TestGetRoleCallsSystemRPCAndMapsResponse(t *testing.T) {
	rpc := &roleQuerySystemRPCStub{getResponse: &systemclient.GetRoleResponse{
		Role: &systemclient.RoleInfo{Id: 9, Code: "auditor", Name: "Auditor"},
	}}
	response, err := NewGetRoleLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: rpc,
	}).GetRole(&types.IDReq{Id: 9})
	if err != nil {
		t.Fatalf("get role: %v", err)
	}
	if rpc.getRequest == nil || rpc.getRequest.Id != 9 ||
		response.Id != 9 || response.Code != "auditor" {
		t.Fatalf("unexpected role RPC mapping: request=%+v response=%+v", rpc.getRequest, response)
	}

	for _, request := range []*types.IDReq{nil, {}} {
		if _, err := NewGetRoleLogic(context.Background(), &svc.ServiceContext{}).
			GetRole(request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("request %+v error = %v, want invalid argument", request, err)
		}
	}
	if _, err := NewGetRoleLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: &roleQuerySystemRPCStub{getResponse: &systemclient.GetRoleResponse{}},
	}).GetRole(&types.IDReq{Id: 1}); err == nil {
		t.Fatal("empty role RPC response was accepted")
	}
}

func TestGetRoleAPIsCallsSystemRPCAndCopiesIDs(t *testing.T) {
	rpc := &roleQuerySystemRPCStub{
		apiResponse: &systemclient.GetRoleAPIsResponse{ApiIds: []int64{11, 12}},
	}
	response, err := NewGetRoleAPIsLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: rpc,
	}).GetRoleAPIs(&types.GetRoleAPIsReq{RoleId: 7})
	if err != nil {
		t.Fatalf("get role APIs: %v", err)
	}
	if rpc.apiRequest == nil || rpc.apiRequest.RoleId != 7 ||
		len(response.ApiIds) != 2 || response.ApiIds[1] != 12 {
		t.Fatalf("unexpected role API RPC mapping: request=%+v response=%+v", rpc.apiRequest, response)
	}
	rpc.apiResponse.ApiIds[0] = 99
	if response.ApiIds[0] != 11 {
		t.Fatal("API response reused mutable RPC API id storage")
	}

	if _, err := NewGetRoleAPIsLogic(context.Background(), &svc.ServiceContext{}).
		GetRoleAPIs(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil role API request error = %v, want invalid argument", err)
	}
	if _, err := NewGetRoleAPIsLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: &roleQuerySystemRPCStub{},
	}).GetRoleAPIs(&types.GetRoleAPIsReq{RoleId: 1}); err == nil {
		t.Fatal("empty role API RPC response was accepted")
	}
}

func TestRoleQueryLogicPreservesRPCErrors(t *testing.T) {
	rpcErr := errors.New("RPC unavailable")
	rpc := &roleQuerySystemRPCStub{err: rpcErr}
	if _, err := NewListRolesLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc}).
		ListRoles(&types.RoleListReq{}); !errors.Is(err, rpcErr) {
		t.Fatalf("list roles error = %v, want %v", err, rpcErr)
	}
	if _, err := NewGetRoleLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc}).
		GetRole(&types.IDReq{Id: 1}); !errors.Is(err, rpcErr) {
		t.Fatalf("get role error = %v, want %v", err, rpcErr)
	}
	if _, err := NewGetRoleAPIsLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc}).
		GetRoleAPIs(&types.GetRoleAPIsReq{RoleId: 1}); !errors.Is(err, rpcErr) {
		t.Fatalf("get role APIs error = %v, want %v", err, rpcErr)
	}
}

var _ systemclient.System = (*roleQuerySystemRPCStub)(nil)
