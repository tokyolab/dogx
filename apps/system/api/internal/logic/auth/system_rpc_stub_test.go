package auth

import (
	"context"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
)

type systemRPCStub struct {
	systemclient.System
	request               *systemclient.LoginRequest
	refreshRequest        *systemclient.RefreshCredentialsRequest
	currentUserRequest    *systemclient.CurrentUserRequest
	revokeRequest         *systemclient.RevokeSessionRequest
	revokeAllRequest      *systemclient.RevokeUserSessionsRequest
	changePasswordRequest *systemclient.ChangePasswordRequest
	response              *systemclient.LoginResponse
	currentUserResponse   *systemclient.CurrentUserResponse
	navigationResponse    *systemclient.ListNavigationMenusResponse
	navigationCalls       int
	err                   error
}

func (s *systemRPCStub) RefreshCredentials(
	_ context.Context,
	request *systemclient.RefreshCredentialsRequest,
	_ ...grpc.CallOption,
) (*systemclient.LoginResponse, error) {
	s.refreshRequest = request
	return s.response, s.err
}

func (s *systemRPCStub) GetCurrentUser(
	_ context.Context,
	request *systemclient.CurrentUserRequest,
	_ ...grpc.CallOption,
) (*systemclient.CurrentUserResponse, error) {
	s.currentUserRequest = request
	return s.currentUserResponse, s.err
}

func (s *systemRPCStub) RevokeSession(
	_ context.Context,
	request *systemclient.RevokeSessionRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	s.revokeRequest = request
	return &systemclient.EmptyResponse{}, s.err
}

func (s *systemRPCStub) RevokeUserSessions(
	_ context.Context,
	request *systemclient.RevokeUserSessionsRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	s.revokeAllRequest = request
	return &systemclient.EmptyResponse{}, s.err
}

func (s *systemRPCStub) ChangePassword(
	_ context.Context,
	request *systemclient.ChangePasswordRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	s.changePasswordRequest = request
	return &systemclient.EmptyResponse{}, s.err
}

func (s *systemRPCStub) Login(
	_ context.Context,
	request *systemclient.LoginRequest,
	_ ...grpc.CallOption,
) (*systemclient.LoginResponse, error) {
	s.request = request
	return s.response, s.err
}

func (s *systemRPCStub) ListNavigationMenus(_ context.Context, _ *systemclient.ListNavigationMenusRequest, _ ...grpc.CallOption) (*systemclient.ListNavigationMenusResponse, error) {
	s.navigationCalls++
	return s.navigationResponse, s.err
}
