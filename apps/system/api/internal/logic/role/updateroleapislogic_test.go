package role

import (
	"context"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type roleSystemRPCStub struct {
	systemclient.System
	request *systemclient.ReplaceRoleAPIsRequest
	err     error
}

func (s *roleSystemRPCStub) ReplaceRoleAPIs(
	_ context.Context,
	request *systemclient.ReplaceRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	s.request = request
	return &systemclient.EmptyResponse{}, s.err
}

func TestUpdateRoleAPIsCallsSystemRPC(t *testing.T) {
	rpc := &roleSystemRPCStub{}
	logic := NewUpdateRoleAPIsLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc})
	response, err := logic.UpdateRoleAPIs(&types.UpdateRoleAPIsReq{RoleId: 7, ApiIds: []int64{1, 2}})
	if err != nil {
		t.Fatalf("update role APIs: %v", err)
	}
	if response == nil || rpc.request == nil || rpc.request.RoleId != 7 || len(rpc.request.ApiIds) != 2 {
		t.Fatalf("unexpected RPC request: response=%+v request=%+v", response, rpc.request)
	}
}

func TestUpdateRoleAPIsRejectsInvalidRole(t *testing.T) {
	logic := NewUpdateRoleAPIsLogic(context.Background(), &svc.ServiceContext{})
	if _, err := logic.UpdateRoleAPIs(&types.UpdateRoleAPIsReq{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unexpected validation error: %v", err)
	}
}
