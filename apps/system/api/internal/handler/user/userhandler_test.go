package user

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	systemsubcode "github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	commonresponse "github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type userHandlerRPCStub struct {
	systemclient.System
	request *systemclient.ListUsersRequest
	called  string
	err     error
}

func (s *userHandlerRPCStub) ListUsers(_ context.Context, req *systemclient.ListUsersRequest, _ ...grpc.CallOption) (*systemclient.ListUsersResponse, error) {
	s.request = req
	return &systemclient.ListUsersResponse{}, nil
}

func (s *userHandlerRPCStub) CreateUser(_ context.Context, _ *systemclient.CreateUserRequest, _ ...grpc.CallOption) (*systemclient.CreateUserResponse, error) {
	s.called = "CreateUser"
	return nil, s.err
}

func (s *userHandlerRPCStub) UpdateUser(_ context.Context, _ *systemclient.UpdateUserRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "UpdateUser"
	return nil, s.err
}

func (s *userHandlerRPCStub) UpdateUserStatus(_ context.Context, _ *systemclient.UpdateUserStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "UpdateUserStatus"
	return nil, s.err
}

func (s *userHandlerRPCStub) DeleteUser(_ context.Context, _ *systemclient.DeleteUserRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "DeleteUser"
	return nil, s.err
}

func (s *userHandlerRPCStub) ReplaceUserRoles(_ context.Context, _ *systemclient.ReplaceUserRolesRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "ReplaceUserRoles"
	return nil, s.err
}

func (s *userHandlerRPCStub) ResetUserPassword(_ context.Context, _ *systemclient.ResetUserPasswordRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "ResetUserPassword"
	return nil, s.err
}

func setupUserResponseHandlers(t *testing.T) {
	t.Helper()
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	httpx.SetValidator(requestvalidator.New())
	t.Cleanup(func() {
		httpx.SetOkHandler(nil)
		httpx.SetErrorHandlerCtx(nil)
		httpx.SetValidator(nil)
	})
}

func TestUserListHTTPPreservesOptionalDisabledStatus(t *testing.T) {
	setupUserResponseHandlers(t)
	for _, tc := range []struct {
		name    string
		body    string
		present bool
		status  int32
	}{
		{name: "unfiltered", body: `{"page":1,"pageSize":200}`},
		{name: "disabled", body: `{"page":1,"pageSize":200,"status":0}`, present: true, status: 0},
		{name: "enabled", body: `{"page":1,"pageSize":200,"status":1}`, present: true, status: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpc := &userHandlerRPCStub{}
			req := httptest.NewRequest(http.MethodPost, "/user/list", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			ListUsersHandler(&svc.ServiceContext{SystemRpc: rpc})(response, req)
			if response.Code != http.StatusOK || rpc.request == nil {
				t.Fatalf("list: status=%d response=%s", response.Code, response.Body.String())
			}
			if (rpc.request.Status != nil) != tc.present || (tc.present && *rpc.request.Status != tc.status) {
				t.Fatalf("status filter lost: %+v", rpc.request)
			}
			var envelope struct {
				Code int `json:"code"`
				Data struct {
					Items []json.RawMessage `json:"items"`
				} `json:"data"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || envelope.Code != 0 || envelope.Data.Items == nil {
				t.Fatalf("empty items must be an array: %s error=%v", response.Body.String(), err)
			}
		})
	}
}

func TestUserHTTPRejectsInvalidFieldsBeforeRPC(t *testing.T) {
	setupUserResponseHandlers(t)
	for _, tc := range []struct {
		name    string
		body    string
		handler func(*svc.ServiceContext) http.HandlerFunc
	}{
		{name: "invalid status", body: `{"page":1,"pageSize":20,"status":2}`, handler: ListUsersHandler},
		{name: "truncating status", body: `{"page":1,"pageSize":20,"status":65536}`, handler: ListUsersHandler},
		{name: "page size limit", body: `{"page":1,"pageSize":201}`, handler: ListUsersHandler},
		{name: "malformed json", body: `{"page":"wrong","pageSize":20}`, handler: ListUsersHandler},
		{name: "invalid role id", body: `{"id":1,"roleIds":[0]}`, handler: UpdateUserRolesHandler},
		{name: "short reset password", body: `{"id":1,"password":"short"}`, handler: ResetUserPasswordHandler},
		{name: "missing create nickname", body: `{"username":"alice","nickname":"","password":"abcdefghijkl","status":1,"roleIds":[]}`, handler: CreateUserHandler},
		{name: "missing update nickname", body: `{"id":1,"nickname":""}`, handler: UpdateUserHandler},
		{name: "invalid mutation status", body: `{"id":1,"status":2}`, handler: UpdateUserStatusHandler},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// No RPC dependency: accepted invalid input would panic instead of passing.
			req := httptest.NewRequest(http.MethodPost, "/user/test", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			tc.handler(&svc.ServiceContext{})(response, req)
			var envelope struct {
				Subcode string `json:"subcode"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || response.Code != http.StatusBadRequest || envelope.Subcode != "common.invalid_request" {
				t.Fatalf("invalid request accepted: status=%d body=%s error=%v", response.Code, response.Body.String(), err)
			}
		})
	}
}

func TestUserMutationHTTPErrorContracts(t *testing.T) {
	setupUserResponseHandlers(t)
	for _, operation := range []struct {
		name    string
		path    string
		body    string
		reason  string
		handler func(*svc.ServiceContext) http.HandlerFunc
	}{
		{"CreateUser", "/user/create", `{"username":"alice","nickname":"Alice","password":"abcdefghijkl","status":1,"roleIds":[]}`, systemsubcode.UserUsernameExists, CreateUserHandler},
		{"UpdateUser", "/user/update", `{"id":9,"nickname":"Alice"}`, systemsubcode.UserEmailExists, UpdateUserHandler},
		{"UpdateUserStatus", "/user/status/update", `{"id":9,"status":0}`, systemsubcode.UserSuperAdminProtected, UpdateUserStatusHandler},
		{"DeleteUser", "/user/delete", `{"id":9}`, systemsubcode.UserSelfProtected, DeleteUserHandler},
		{"ReplaceUserRoles", "/user/role/update", `{"id":9,"roleIds":[8]}`, systemsubcode.UserRoleUnavailable, UpdateUserRolesHandler},
		{"ResetUserPassword", "/user/password/reset", `{"id":9,"password":"abcdefghijkl"}`, systemsubcode.UserSuperAdminProtected, ResetUserPasswordHandler},
	} {
		const diagnostic = "private-rpc-diagnostic"
		businessStatus, err := status.New(codes.Code(bizerror.DefaultCode), diagnostic).
			WithDetails(&errdetails.ErrorInfo{Reason: operation.reason})
		if err != nil {
			t.Fatal(err)
		}
		for _, scenario := range []struct {
			name       string
			body       string
			rpcErr     error
			wantStatus int
			wantCode   uint32
			wantReason string
			wantRPC    string
		}{
			{"malformed JSON", `{"id":`, nil, http.StatusBadRequest, http.StatusBadRequest, "common.invalid_request", ""},
			{"business failure", operation.body, businessStatus.Err(), http.StatusOK, bizerror.DefaultCode, operation.reason, operation.name},
			{"RPC unavailable", operation.body, status.Error(codes.Unavailable, diagnostic), http.StatusServiceUnavailable, http.StatusServiceUnavailable, "common.service_unavailable", operation.name},
		} {
			t.Run(operation.name+"/"+scenario.name, func(t *testing.T) {
				rpc := &userHandlerRPCStub{err: scenario.rpcErr}
				req := httptest.NewRequest(http.MethodPost, operation.path, strings.NewReader(scenario.body))
				req.Header.Set("Content-Type", "application/json")
				ctx := context.WithValue(req.Context(), "userId", int64(42))
				ctx = context.WithValue(ctx, "sessionId", "handler-test-session")
				ctx = context.WithValue(ctx, "isSuperAdmin", false)
				response := httptest.NewRecorder()
				operation.handler(&svc.ServiceContext{SystemRpc: rpc})(response, req.WithContext(ctx))
				var envelope struct {
					Code    uint32          `json:"code"`
					Subcode string          `json:"subcode"`
					Message string          `json:"message"`
					Data    json.RawMessage `json:"data"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
					t.Fatalf("decode error response: %v body=%s", err, response.Body.String())
				}
				if response.Code != scenario.wantStatus || envelope.Code != scenario.wantCode ||
					envelope.Subcode != scenario.wantReason || string(envelope.Data) != "null" || envelope.Message == "" {
					t.Fatalf("unexpected error contract: status=%d body=%s", response.Code, response.Body.String())
				}
				if rpc.called != scenario.wantRPC {
					t.Fatalf("called RPC=%q, want %q", rpc.called, scenario.wantRPC)
				}
				if strings.Contains(response.Body.String(), diagnostic) {
					t.Fatal("internal RPC diagnostic leaked into the HTTP response")
				}
			})
		}
	}
}
