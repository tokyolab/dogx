package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"google.golang.org/grpc"
)

type systemRPCStub struct {
	systemclient.System
	request             *systemclient.LoginRequest
	refreshRequest      *systemclient.RefreshCredentialsRequest
	currentUserRequest  *systemclient.CurrentUserRequest
	revokeRequest       *systemclient.RevokeSessionRequest
	revokeAllRequest    *systemclient.RevokeUserSessionsRequest
	response            *systemclient.LoginResponse
	currentUserResponse *systemclient.CurrentUserResponse
	err                 error
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

func (s *systemRPCStub) Login(
	_ context.Context,
	request *systemclient.LoginRequest,
	_ ...grpc.CallOption,
) (*systemclient.LoginResponse, error) {
	s.request = request
	return s.response, s.err
}

func TestLogin(t *testing.T) {
	rpc := &systemRPCStub{response: &systemclient.LoginResponse{
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		ExpiresIn:    900,
	}}
	logic := NewLoginLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc})

	response, err := logic.Login(&types.LoginReq{Username: "admin", Password: "password"})
	if err != nil {
		t.Fatalf("login returned error: %v", err)
	}
	if rpc.request.Username != "admin" || rpc.request.Password != "password" {
		t.Fatalf("unexpected RPC request: %+v", rpc.request)
	}
	if response.AccessToken != "access-token" || response.RefreshToken != "refresh-token" || response.ExpiresIn != 900 {
		t.Fatalf("unexpected API response: %+v", response)
	}
}

func TestLoginReturnsRPCError(t *testing.T) {
	rpcErr := errors.New("rpc unavailable")
	logic := NewLoginLogic(context.Background(), &svc.ServiceContext{
		SystemRpc: &systemRPCStub{err: rpcErr},
	})

	if _, err := logic.Login(&types.LoginReq{Username: "admin", Password: "password"}); !errors.Is(err, rpcErr) {
		t.Fatalf("expected RPC error, got: %v", err)
	}
}

func TestRefreshToken(t *testing.T) {
	rpc := &systemRPCStub{response: &systemclient.LoginResponse{
		AccessToken:  "new-access-token",
		RefreshToken: "new-refresh-token",
		ExpiresIn:    900,
	}}
	logic := NewRefreshTokenLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc})

	response, err := logic.RefreshToken(&types.RefreshTokenReq{RefreshToken: "old-refresh-token"})
	if err != nil {
		t.Fatalf("refresh token: %v", err)
	}
	if rpc.refreshRequest.RefreshToken != "old-refresh-token" || response.AccessToken != "new-access-token" ||
		response.RefreshToken != "new-refresh-token" {
		t.Fatalf("unexpected refresh result: request=%+v response=%+v", rpc.refreshRequest, response)
	}
}

func TestCurrentUserAndLogoutUseJWTIdentity(t *testing.T) {
	ctx := authenticatedTestContext()
	rpc := &systemRPCStub{currentUserResponse: &systemclient.CurrentUserResponse{
		Id:       42,
		Username: "admin",
		Nickname: "Administrator",
	}}
	svcCtx := &svc.ServiceContext{SystemRpc: rpc}

	current, err := NewCurrentUserLogic(ctx, svcCtx).CurrentUser()
	if err != nil {
		t.Fatalf("get current user: %v", err)
	}
	if current.Id != 42 || rpc.currentUserRequest.UserId != 42 {
		t.Fatalf("unexpected current user: response=%+v request=%+v", current, rpc.currentUserRequest)
	}
	if _, err := NewLogoutLogic(ctx, svcCtx).Logout(); err != nil {
		t.Fatalf("logout: %v", err)
	}
	if rpc.revokeRequest.UserId != 42 || rpc.revokeRequest.SessionId != "session-id" {
		t.Fatalf("unexpected logout request: %+v", rpc.revokeRequest)
	}
	if _, err := NewLogoutAllLogic(ctx, svcCtx).LogoutAll(); err != nil {
		t.Fatalf("logout all: %v", err)
	}
	if rpc.revokeAllRequest.UserId != 42 {
		t.Fatalf("unexpected logout-all request: %+v", rpc.revokeAllRequest)
	}
}

func TestProtectedLogicRejectsMissingIdentity(t *testing.T) {
	logic := NewCurrentUserLogic(context.Background(), &svc.ServiceContext{SystemRpc: &systemRPCStub{}})
	if _, err := logic.CurrentUser(); err == nil {
		t.Fatal("expected missing JWT identity to be rejected")
	}
}

func authenticatedTestContext() context.Context {
	ctx := context.WithValue(context.Background(), "userId", int64(42))
	return context.WithValue(ctx, "sessionId", "session-id")
}
