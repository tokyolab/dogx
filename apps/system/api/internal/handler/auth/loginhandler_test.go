package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/bizerror"
	commonresponse "github.com/tokyolab/dogx/pkg/response"

	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type handlerSystemRPCStub struct {
	systemclient.System
	response            *systemclient.LoginResponse
	currentUserResponse *systemclient.CurrentUserResponse
	err                 error
}

func (s handlerSystemRPCStub) RefreshCredentials(
	context.Context,
	*systemclient.RefreshCredentialsRequest,
	...grpc.CallOption,
) (*systemclient.LoginResponse, error) {
	return s.response, s.err
}

func (s handlerSystemRPCStub) GetCurrentUser(
	context.Context,
	*systemclient.CurrentUserRequest,
	...grpc.CallOption,
) (*systemclient.CurrentUserResponse, error) {
	return s.currentUserResponse, s.err
}

func (s handlerSystemRPCStub) RevokeSession(
	context.Context,
	*systemclient.RevokeSessionRequest,
	...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	return &systemclient.EmptyResponse{}, s.err
}

func (s handlerSystemRPCStub) RevokeUserSessions(
	context.Context,
	*systemclient.RevokeUserSessionsRequest,
	...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	return &systemclient.EmptyResponse{}, s.err
}

func (s handlerSystemRPCStub) Login(
	context.Context,
	*systemclient.LoginRequest,
	...grpc.CallOption,
) (*systemclient.LoginResponse, error) {
	return s.response, s.err
}

func TestLoginHandler(t *testing.T) {
	setResponseHandlers(t)
	svcCtx := &svc.ServiceContext{SystemRpc: handlerSystemRPCStub{
		response: &systemclient.LoginResponse{
			AccessToken:  "access-token",
			RefreshToken: "refresh-token",
			ExpiresIn:    900,
		},
	}}
	request := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(`{"username":"admin","password":"password"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	LoginHandler(svcCtx).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Code uint32          `json:"code"`
		Data types.LoginResp `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	if response.Code != 0 || response.Data.AccessToken != "access-token" || response.Data.RefreshToken != "refresh-token" {
		t.Fatalf("unexpected login response: %+v", response)
	}
}

func TestLoginHandlerReturnsBusinessError(t *testing.T) {
	setResponseHandlers(t)
	svcCtx := &svc.ServiceContext{SystemRpc: handlerSystemRPCStub{
		err: status.Error(codes.Code(bizerror.DefaultCode), "用户名或密码错误"),
	}}
	request := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(`{"username":"admin","password":"wrong"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	LoginHandler(svcCtx).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	var response commonresponse.Body
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if response.Code != bizerror.DefaultCode || response.Message != "用户名或密码错误" {
		t.Fatalf("unexpected business response: %+v", response)
	}
}

func TestLoginHandlerRejectsInvalidJSON(t *testing.T) {
	setResponseHandlers(t)
	request := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(`{"username":`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	LoginHandler(&svc.ServiceContext{}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestRefreshTokenHandler(t *testing.T) {
	setResponseHandlers(t)
	svcCtx := &svc.ServiceContext{SystemRpc: handlerSystemRPCStub{
		response: &systemclient.LoginResponse{
			AccessToken:  "new-access-token",
			RefreshToken: "new-refresh-token",
			ExpiresIn:    900,
		},
	}}
	request := httptest.NewRequest(http.MethodPost, "/auth/refresh", strings.NewReader(`{"refreshToken":"old-refresh-token"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	RefreshTokenHandler(svcCtx).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "new-access-token") {
		t.Fatalf("unexpected refresh response: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestProtectedAuthHandlers(t *testing.T) {
	setResponseHandlers(t)
	svcCtx := &svc.ServiceContext{SystemRpc: handlerSystemRPCStub{
		currentUserResponse: &systemclient.CurrentUserResponse{
			Id:       42,
			Username: "admin",
			Nickname: "Administrator",
		},
	}}

	tests := []struct {
		name    string
		path    string
		handler http.HandlerFunc
	}{
		{name: "current user", path: "/auth/me", handler: CurrentUserHandler(svcCtx)},
		{name: "logout", path: "/auth/logout", handler: LogoutHandler(svcCtx)},
		{name: "logout all", path: "/auth/logout-all", handler: LogoutAllHandler(svcCtx)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), "userId", int64(42))
			ctx = context.WithValue(ctx, "sessionId", "session-id")
			request := httptest.NewRequest(http.MethodPost, test.path, nil).WithContext(ctx)
			recorder := httptest.NewRecorder()

			test.handler.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusOK {
				t.Fatalf("unexpected response: status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func setResponseHandlers(t *testing.T) {
	t.Helper()
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	t.Cleanup(func() {
		httpx.SetOkHandler(nil)
		httpx.SetErrorHandlerCtx(nil)
	})
}
