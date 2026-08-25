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
	request *systemclient.ReplaceRoleAPIsRequest
	err     error
}

func (s *roleHandlerSystemRPCStub) ReplaceRoleAPIs(
	_ context.Context,
	request *systemclient.ReplaceRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	s.request = request
	return &systemclient.EmptyResponse{}, s.err
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

func newUpdateRoleAPIsRequest(body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/role/api/update", strings.NewReader(body))
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
