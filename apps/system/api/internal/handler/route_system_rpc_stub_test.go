package handler

import (
	"context"
	"net/http"

	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
)

type routeSystemRPCStub struct {
	navigationRequest *systemclient.ListNavigationMenusRequest
	systemclient.System
	order                *[]string
	called               string
	request              *systemclient.ReplaceRoleAPIsRequest
	listRolesRequest     *systemclient.ListRolesRequest
	getRoleRequest       *systemclient.GetRoleRequest
	listAPIsRequest      *systemclient.ListAPIsRequest
	getRoleAPIsRequest   *systemclient.GetRoleAPIsRequest
	profileRequest       *systemclient.CurrentUserRequest
	updateProfileRequest *systemclient.UpdateProfileRequest
}

func (s *routeSystemRPCStub) GetProfile(_ context.Context, req *systemclient.CurrentUserRequest, _ ...grpc.CallOption) (*systemclient.ProfileResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.profileRequest = req
	return &systemclient.ProfileResponse{Username: "alice", Roles: []string{}}, nil
}

func (s *routeSystemRPCStub) UpdateProfile(_ context.Context, req *systemclient.UpdateProfileRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.updateProfileRequest = req
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) CreateRole(
	_ context.Context,
	_ *systemclient.CreateRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.CreateRoleResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "CreateRole"
	return &systemclient.CreateRoleResponse{Id: 13}, nil
}

func (s *routeSystemRPCStub) UpdateRole(
	_ context.Context,
	_ *systemclient.UpdateRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateRole"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) UpdateRoleStatus(
	_ context.Context,
	_ *systemclient.UpdateRoleStatusRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateRoleStatus"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) DeleteRole(
	_ context.Context,
	_ *systemclient.DeleteRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "DeleteRole"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) ReplaceRoleAPIs(
	_ context.Context,
	request *systemclient.ReplaceRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ReplaceRoleAPIs"
	s.request = request
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) ListRoles(
	_ context.Context,
	request *systemclient.ListRolesRequest,
	_ ...grpc.CallOption,
) (*systemclient.ListRolesResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ListRoles"
	s.listRolesRequest = request
	return &systemclient.ListRolesResponse{
		Items: []*systemclient.RoleInfo{{Id: 7, Code: "operator", Name: "Operator"}},
		Total: 1,
	}, nil
}

func (s *routeSystemRPCStub) GetRole(
	_ context.Context,
	request *systemclient.GetRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.GetRoleResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "GetRole"
	s.getRoleRequest = request
	return &systemclient.GetRoleResponse{
		Role: &systemclient.RoleInfo{Id: request.Id, Code: "operator", Name: "Operator"},
	}, nil
}

func (s *routeSystemRPCStub) ListAPIs(
	_ context.Context,
	request *systemclient.ListAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.ListAPIsResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ListAPIs"
	s.listAPIsRequest = request
	return &systemclient.ListAPIsResponse{
		Items: []*systemclient.APIInfo{{
			Id:          11,
			ServiceName: "system-api",
			ApiGroup:    "角色管理",
			Name:        "查询角色",
			Path:        "/role/get",
			Method:      http.MethodPost,
			Status:      1,
		}},
	}, nil
}

func (s *routeSystemRPCStub) GetRoleAPIs(
	_ context.Context,
	request *systemclient.GetRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.GetRoleAPIsResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "GetRoleAPIs"
	s.getRoleAPIsRequest = request
	return &systemclient.GetRoleAPIsResponse{ApiIds: []int64{11, 12}}, nil
}

func (s *routeSystemRPCStub) ListNavigationMenus(_ context.Context, request *systemclient.ListNavigationMenusRequest, _ ...grpc.CallOption) (*systemclient.ListNavigationMenusResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.navigationRequest = request
	s.called = "ListNavigationMenus"
	return &systemclient.ListNavigationMenusResponse{}, nil
}

func (s *routeSystemRPCStub) ListUsers(_ context.Context, _ *systemclient.ListUsersRequest, _ ...grpc.CallOption) (*systemclient.ListUsersResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ListUsers"
	return &systemclient.ListUsersResponse{Items: []*systemclient.UserInfo{}, Total: 0}, nil
}

func (s *routeSystemRPCStub) GetUser(_ context.Context, _ *systemclient.GetUserRequest, _ ...grpc.CallOption) (*systemclient.GetUserResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "GetUser"
	return &systemclient.GetUserResponse{User: &systemclient.UserInfo{Id: 9, Username: "alice", Nickname: "Alice"}}, nil
}

func (s *routeSystemRPCStub) CreateUser(_ context.Context, _ *systemclient.CreateUserRequest, _ ...grpc.CallOption) (*systemclient.CreateUserResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "CreateUser"
	return &systemclient.CreateUserResponse{Id: 9}, nil
}

func (s *routeSystemRPCStub) UpdateUser(_ context.Context, _ *systemclient.UpdateUserRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateUser"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) UpdateUserStatus(_ context.Context, _ *systemclient.UpdateUserStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateUserStatus"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) DeleteUser(_ context.Context, _ *systemclient.DeleteUserRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "DeleteUser"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) ReplaceUserRoles(_ context.Context, _ *systemclient.ReplaceUserRolesRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ReplaceUserRoles"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) ResetUserPassword(_ context.Context, _ *systemclient.ResetUserPasswordRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ResetUserPassword"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) ListUserRoleOptions(_ context.Context, _ *systemclient.ListRolesRequest, _ ...grpc.CallOption) (*systemclient.ListUserRoleOptionsResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ListUserRoleOptions"
	return &systemclient.ListUserRoleOptionsResponse{Items: []*systemclient.UserRoleInfo{}}, nil
}

func (s *routeSystemRPCStub) ListDepartments(_ context.Context, _ *systemclient.ListDepartmentsRequest, _ ...grpc.CallOption) (*systemclient.ListDepartmentsResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ListDepartments"
	return &systemclient.ListDepartmentsResponse{}, nil
}

func (s *routeSystemRPCStub) GetDepartment(_ context.Context, request *systemclient.GetDepartmentRequest, _ ...grpc.CallOption) (*systemclient.GetDepartmentResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "GetDepartment"
	return &systemclient.GetDepartmentResponse{Department: &systemclient.DepartmentInfo{Id: request.Id, Name: "研发部"}}, nil
}

func (s *routeSystemRPCStub) CreateDepartment(_ context.Context, _ *systemclient.CreateDepartmentRequest, _ ...grpc.CallOption) (*systemclient.CreateDepartmentResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "CreateDepartment"
	return &systemclient.CreateDepartmentResponse{Id: 9}, nil
}

func (s *routeSystemRPCStub) UpdateDepartment(_ context.Context, _ *systemclient.UpdateDepartmentRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateDepartment"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) UpdateDepartmentStatus(_ context.Context, _ *systemclient.UpdateDepartmentStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateDepartmentStatus"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) DeleteDepartment(_ context.Context, _ *systemclient.DeleteDepartmentRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "DeleteDepartment"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) CreateMenu(_ context.Context, _ *systemclient.CreateMenuRequest, _ ...grpc.CallOption) (*systemclient.CreateMenuResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "CreateMenu"
	return &systemclient.CreateMenuResponse{Id: 9}, nil
}

func (s *routeSystemRPCStub) UpdateMenu(_ context.Context, _ *systemclient.UpdateMenuRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateMenu"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) UpdateMenuStatus(_ context.Context, _ *systemclient.UpdateMenuStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateMenuStatus"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) DeleteMenu(_ context.Context, _ *systemclient.DeleteMenuRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "DeleteMenu"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) GetMenu(_ context.Context, _ *systemclient.GetMenuRequest, _ ...grpc.CallOption) (*systemclient.GetMenuResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "GetMenu"
	return &systemclient.GetMenuResponse{Menu: &systemclient.MenuInfo{Id: 9, Menu: &systemclient.MenuFields{Name: "Page"}}}, nil
}

func (s *routeSystemRPCStub) ListMenus(_ context.Context, _ *systemclient.ListMenusRequest, _ ...grpc.CallOption) (*systemclient.ListMenusResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ListMenus"
	return &systemclient.ListMenusResponse{}, nil
}

func (s *routeSystemRPCStub) GetRoleMenus(_ context.Context, _ *systemclient.GetRoleMenusRequest, _ ...grpc.CallOption) (*systemclient.GetRoleMenusResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "GetRoleMenus"
	return &systemclient.GetRoleMenusResponse{}, nil
}
func (s *routeSystemRPCStub) ReplaceRoleMenus(_ context.Context, _ *systemclient.ReplaceRoleMenusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ReplaceRoleMenus"
	return &systemclient.EmptyResponse{}, nil
}

var _ systemclient.System = (*routeSystemRPCStub)(nil)
