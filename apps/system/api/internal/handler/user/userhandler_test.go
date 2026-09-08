package user

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	commonresponse "github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
)

type userHandlerRPCStub struct {
	systemclient.System
	request *systemclient.ListUsersRequest
}

func (s *userHandlerRPCStub) ListUsers(_ context.Context, req *systemclient.ListUsersRequest, _ ...grpc.CallOption) (*systemclient.ListUsersResponse, error) {
	s.request = req
	return &systemclient.ListUsersResponse{}, nil
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
