package role

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type roleMutationRPCStub struct {
	systemclient.System
	createRequest *systemclient.CreateRoleRequest
	updateRequest *systemclient.UpdateRoleRequest
	statusRequest *systemclient.UpdateRoleStatusRequest
	deleteRequest *systemclient.DeleteRoleRequest
	err           error
}

func (s *roleMutationRPCStub) CreateRole(
	_ context.Context,
	request *systemclient.CreateRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.CreateRoleResponse, error) {
	s.createRequest = request
	return &systemclient.CreateRoleResponse{Id: 31}, s.err
}

func (s *roleMutationRPCStub) UpdateRole(
	_ context.Context,
	request *systemclient.UpdateRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	s.updateRequest = request
	return &systemclient.EmptyResponse{}, s.err
}

func (s *roleMutationRPCStub) UpdateRoleStatus(
	_ context.Context,
	request *systemclient.UpdateRoleStatusRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	s.statusRequest = request
	return &systemclient.EmptyResponse{}, s.err
}

func (s *roleMutationRPCStub) DeleteRole(
	_ context.Context,
	request *systemclient.DeleteRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	s.deleteRequest = request
	return &systemclient.EmptyResponse{}, s.err
}

func TestRoleMutationLogicMapsHTTPContractsToRPC(t *testing.T) {
	rpc := &roleMutationRPCStub{}
	serviceContext := &svc.ServiceContext{SystemRpc: rpc}
	created, err := NewCreateRoleLogic(context.Background(), serviceContext).CreateRole(&types.CreateRoleReq{
		Code: "operator", Name: "Operator", Description: "Operations", Sort: 10, Status: 1,
	})
	if err != nil {
		t.Fatalf("create role: %v", err)
	}
	if created.Id != 31 || rpc.createRequest == nil || rpc.createRequest.Code != "operator" ||
		rpc.createRequest.Sort != 10 || rpc.createRequest.Status != 1 {
		t.Fatalf("unexpected create mapping: response=%+v request=%+v", created, rpc.createRequest)
	}

	if _, err := NewUpdateRoleLogic(context.Background(), serviceContext).UpdateRole(&types.UpdateRoleReq{
		Id: 31, Code: "auditor", Name: "Auditor", Description: "Audit", Sort: 20,
	}); err != nil {
		t.Fatalf("update role: %v", err)
	}
	if rpc.updateRequest == nil || rpc.updateRequest.Id != 31 || rpc.updateRequest.Code != "auditor" ||
		rpc.updateRequest.Sort != 20 {
		t.Fatalf("unexpected update mapping: %+v", rpc.updateRequest)
	}

	if _, err := NewUpdateRoleStatusLogic(context.Background(), serviceContext).
		UpdateRoleStatus(&types.UpdateRoleStatusReq{Id: 31, Status: 0}); err != nil {
		t.Fatalf("update role status: %v", err)
	}
	if rpc.statusRequest == nil || rpc.statusRequest.Id != 31 || rpc.statusRequest.Status != 0 {
		t.Fatalf("unexpected status mapping: %+v", rpc.statusRequest)
	}

	if _, err := NewDeleteRoleLogic(context.Background(), serviceContext).
		DeleteRole(&types.IDReq{Id: 31}); err != nil {
		t.Fatalf("delete role: %v", err)
	}
	if rpc.deleteRequest == nil || rpc.deleteRequest.Id != 31 {
		t.Fatalf("unexpected delete mapping: %+v", rpc.deleteRequest)
	}
}

func TestRoleMutationLogicRejectsInvalidRequestsAndPropagatesRPCError(t *testing.T) {
	rpc := &roleMutationRPCStub{}
	serviceContext := &svc.ServiceContext{SystemRpc: rpc}
	invalidCreates := []*types.CreateRoleReq{
		nil,
		{Code: "", Name: "Role", Status: 1},
		{Code: "role", Name: "", Status: 1},
		{Code: "role", Name: "Role", Sort: -1, Status: 1},
		{Code: "role", Name: "Role", Sort: math.MaxInt32 + 1, Status: 1},
		{Code: "role", Name: "Role", Status: 2},
	}
	for _, request := range invalidCreates {
		if _, err := NewCreateRoleLogic(context.Background(), serviceContext).
			CreateRole(request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("create request %+v error = %v, want invalid argument", request, err)
		}
	}
	if _, err := NewUpdateRoleLogic(context.Background(), serviceContext).
		UpdateRole(&types.UpdateRoleReq{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid update error = %v, want invalid argument", err)
	}
	if _, err := NewUpdateRoleStatusLogic(context.Background(), serviceContext).
		UpdateRoleStatus(&types.UpdateRoleStatusReq{Id: 1, Status: 2}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid status error = %v, want invalid argument", err)
	}
	if _, err := NewDeleteRoleLogic(context.Background(), serviceContext).
		DeleteRole(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid deletion error = %v, want invalid argument", err)
	}

	rpc.err = errors.New("RPC unavailable")
	if _, err := NewCreateRoleLogic(context.Background(), serviceContext).CreateRole(&types.CreateRoleReq{
		Code: "role", Name: "Role", Status: 1,
	}); !errors.Is(err, rpc.err) {
		t.Fatalf("create RPC error = %v, want %v", err, rpc.err)
	}
	if _, err := NewUpdateRoleLogic(context.Background(), serviceContext).UpdateRole(&types.UpdateRoleReq{
		Id: 1, Code: "role", Name: "Role",
	}); !errors.Is(err, rpc.err) {
		t.Fatalf("update RPC error = %v, want %v", err, rpc.err)
	}
	if _, err := NewUpdateRoleStatusLogic(context.Background(), serviceContext).
		UpdateRoleStatus(&types.UpdateRoleStatusReq{Id: 1, Status: 0}); !errors.Is(err, rpc.err) {
		t.Fatalf("status RPC error = %v, want %v", err, rpc.err)
	}
	if _, err := NewDeleteRoleLogic(context.Background(), serviceContext).
		DeleteRole(&types.IDReq{Id: 1}); !errors.Is(err, rpc.err) {
		t.Fatalf("delete RPC error = %v, want %v", err, rpc.err)
	}
}

var _ systemclient.System = (*roleMutationRPCStub)(nil)
