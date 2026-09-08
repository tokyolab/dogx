package handler

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
)

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
