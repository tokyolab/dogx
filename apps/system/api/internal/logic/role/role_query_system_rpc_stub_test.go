package role

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"google.golang.org/grpc"
)

type roleQuerySystemRPCStub struct {
	menuRequest        *systemclient.GetRoleMenusRequest
	menuResponse       *systemclient.GetRoleMenusResponse
	replaceMenuRequest *systemclient.ReplaceRoleMenusRequest
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

func (s *roleQuerySystemRPCStub) GetRoleMenus(_ context.Context, req *systemclient.GetRoleMenusRequest, _ ...grpc.CallOption) (*systemclient.GetRoleMenusResponse, error) {
	s.menuRequest = req
	return s.menuResponse, s.err
}
func (s *roleQuerySystemRPCStub) ReplaceRoleMenus(_ context.Context, req *systemclient.ReplaceRoleMenusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.replaceMenuRequest = req
	return &systemclient.EmptyResponse{}, s.err
}

var _ systemclient.System = (*roleQuerySystemRPCStub)(nil)
