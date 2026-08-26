package role

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/bizerror"
	commonresponse "github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type roleHandlerSystemRPCStub struct {
	systemclient.System
	request        *systemclient.ReplaceRoleAPIsRequest
	listRequest    *systemclient.ListRolesRequest
	getRequest     *systemclient.GetRoleRequest
	getAPIsRequest *systemclient.GetRoleAPIsRequest
	err            error
}

func (s *roleHandlerSystemRPCStub) ReplaceRoleAPIs(
	_ context.Context,
	request *systemclient.ReplaceRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	s.request = request
	return &systemclient.EmptyResponse{}, s.err
}

func (s *roleHandlerSystemRPCStub) ListRoles(
	_ context.Context,
	request *systemclient.ListRolesRequest,
	_ ...grpc.CallOption,
) (*systemclient.ListRolesResponse, error) {
	s.listRequest = request
	return &systemclient.ListRolesResponse{
		Items: []*systemclient.RoleInfo{{Id: 7, Code: "operator", Name: "Operator"}},
		Total: 1,
	}, s.err
}

func (s *roleHandlerSystemRPCStub) GetRole(
	_ context.Context,
	request *systemclient.GetRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.GetRoleResponse, error) {
	s.getRequest = request
	return &systemclient.GetRoleResponse{
		Role: &systemclient.RoleInfo{Id: request.Id, Code: "operator", Name: "Operator"},
	}, s.err
}

func (s *roleHandlerSystemRPCStub) GetRoleAPIs(
	_ context.Context,
	request *systemclient.GetRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.GetRoleAPIsResponse, error) {
	s.getAPIsRequest = request
	return &systemclient.GetRoleAPIsResponse{ApiIds: []int64{11, 12}}, s.err
}

func TestUpdateRoleAPIsHandler(t *testing.T) {
	setRoleResponseHandlers(t)
	rpc := &roleHandlerSystemRPCStub{}
	request := newUpdateRoleAPIsRequest(`{"roleId":7,"apiIds":[11,12]}`)
	recorder := httptest.NewRecorder()

	UpdateRoleAPIsHandler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response commonresponse.Body
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode role API response: %v", err)
	}
	if response.Code != commonresponse.SuccessCode || rpc.request == nil ||
		rpc.request.RoleId != 7 || len(rpc.request.ApiIds) != 2 ||
		rpc.request.ApiIds[0] != 11 || rpc.request.ApiIds[1] != 12 {
		t.Fatalf("unexpected response or RPC request: response=%+v request=%+v", response, rpc.request)
	}
}

func TestUpdateRoleAPIsHandlerRejectsInvalidJSON(t *testing.T) {
	setRoleResponseHandlers(t)
	rpc := &roleHandlerSystemRPCStub{}
	request := newUpdateRoleAPIsRequest(`{"roleId":`)
	recorder := httptest.NewRecorder()

	UpdateRoleAPIsHandler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest || rpc.request != nil {
		t.Fatalf("unexpected invalid JSON response: status=%d body=%s request=%+v", recorder.Code, recorder.Body.String(), rpc.request)
	}
}

func TestUpdateRoleAPIsHandlerRejectsInvalidRoleID(t *testing.T) {
	setRoleResponseHandlers(t)
	rpc := &roleHandlerSystemRPCStub{}
	request := newUpdateRoleAPIsRequest(`{"roleId":0,"apiIds":[]}`)
	recorder := httptest.NewRecorder()

	UpdateRoleAPIsHandler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest || rpc.request != nil {
		t.Fatalf("unexpected invalid role response: status=%d body=%s request=%+v", recorder.Code, recorder.Body.String(), rpc.request)
	}
}

func TestUpdateRoleAPIsHandlerReturnsBusinessError(t *testing.T) {
	setRoleResponseHandlers(t)
	rpc := &roleHandlerSystemRPCStub{
		err: status.Error(codes.Code(bizerror.DefaultCode), "角色不存在或已停用"),
	}
	request := newUpdateRoleAPIsRequest(`{"roleId":7,"apiIds":[11]}`)
	recorder := httptest.NewRecorder()

	UpdateRoleAPIsHandler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected business error status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response commonresponse.Body
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode business error response: %v", err)
	}
	if response.Code != bizerror.DefaultCode || response.Message != "角色不存在或已停用" {
		t.Fatalf("unexpected business error response: %+v", response)
	}
}

func TestRoleQueryHandlersParseAndReturnResponses(t *testing.T) {
	setRoleResponseHandlers(t)
	tests := []struct {
		name    string
		path    string
		body    string
		handler func(*svc.ServiceContext) http.HandlerFunc
		assert  func(*testing.T, *roleHandlerSystemRPCStub)
	}{
		{
			name:    "list",
			path:    "/role/list",
			body:    `{"page":1,"pageSize":20,"keyword":"operator"}`,
			handler: ListRolesHandler,
			assert: func(t *testing.T, rpc *roleHandlerSystemRPCStub) {
				if rpc.listRequest == nil || rpc.listRequest.Page != 1 ||
					rpc.listRequest.PageSize != 20 || rpc.listRequest.Keyword != "operator" {
					t.Fatalf("unexpected role list request: %+v", rpc.listRequest)
				}
			},
		},
		{
			name:    "get",
			path:    "/role/get",
			body:    `{"id":7}`,
			handler: GetRoleHandler,
			assert: func(t *testing.T, rpc *roleHandlerSystemRPCStub) {
				if rpc.getRequest == nil || rpc.getRequest.Id != 7 {
					t.Fatalf("unexpected get role request: %+v", rpc.getRequest)
				}
			},
		},
		{
			name:    "get APIs",
			path:    "/role/api/get",
			body:    `{"roleId":7}`,
			handler: GetRoleAPIsHandler,
			assert: func(t *testing.T, rpc *roleHandlerSystemRPCStub) {
				if rpc.getAPIsRequest == nil || rpc.getAPIsRequest.RoleId != 7 {
					t.Fatalf("unexpected get role APIs request: %+v", rpc.getAPIsRequest)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rpc := &roleHandlerSystemRPCStub{}
			request := newRoleHandlerRequest(test.path, test.body)
			recorder := httptest.NewRecorder()

			test.handler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)

			if recorder.Code != http.StatusOK {
				t.Fatalf("unexpected status: %d body=%s", recorder.Code, recorder.Body.String())
			}
			var response commonresponse.Body
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatalf("decode role query response: %v", err)
			}
			if response.Code != commonresponse.SuccessCode {
				t.Fatalf("unexpected role query response: %+v", response)
			}
			test.assert(t, rpc)
		})
	}
}

func TestRoleQueryHandlersRejectInvalidJSON(t *testing.T) {
	setRoleResponseHandlers(t)
	handlers := []struct {
		name    string
		path    string
		handler func(*svc.ServiceContext) http.HandlerFunc
	}{
		{name: "list", path: "/role/list", handler: ListRolesHandler},
		{name: "get", path: "/role/get", handler: GetRoleHandler},
		{name: "get APIs", path: "/role/api/get", handler: GetRoleAPIsHandler},
	}
	for _, test := range handlers {
		t.Run(test.name, func(t *testing.T) {
			rpc := &roleHandlerSystemRPCStub{}
			request := newRoleHandlerRequest(test.path, `{"invalid":`)
			recorder := httptest.NewRecorder()

			test.handler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest ||
				rpc.listRequest != nil || rpc.getRequest != nil || rpc.getAPIsRequest != nil {
				t.Fatalf(
					"unexpected invalid JSON response: status=%d body=%s",
					recorder.Code,
					recorder.Body.String(),
				)
			}
		})
	}
}

func newUpdateRoleAPIsRequest(body string) *http.Request {
	return newRoleHandlerRequest("/role/api/update", body)
}

func newRoleHandlerRequest(path, body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request
}

func setRoleResponseHandlers(t *testing.T) {
	t.Helper()
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	t.Cleanup(func() {
		httpx.SetOkHandler(nil)
		httpx.SetErrorHandlerCtx(nil)
	})
}
