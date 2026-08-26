package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	commonresponse "github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
)

type apiHandlerSystemRPCStub struct {
	systemclient.System
	request *systemclient.ListAPIsRequest
}

func (s *apiHandlerSystemRPCStub) ListAPIs(
	_ context.Context,
	request *systemclient.ListAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.ListAPIsResponse, error) {
	s.request = request
	return &systemclient.ListAPIsResponse{
		Items: []*systemclient.APIInfo{{Id: 11, Name: "查询角色", Method: http.MethodPost}},
	}, nil
}

func TestListAPIsHandlerParsesAndReturnsResponse(t *testing.T) {
	setAPIResponseHandlers(t)
	rpc := &apiHandlerSystemRPCStub{}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/list",
		strings.NewReader(`{"keyword":"role","serviceName":"system-api","group":"角色管理"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	ListAPIsHandler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response commonresponse.Body
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode API list response: %v", err)
	}
	if response.Code != commonresponse.SuccessCode || rpc.request == nil ||
		rpc.request.Keyword != "role" ||
		rpc.request.ServiceName != "system-api" ||
		rpc.request.ApiGroup != "角色管理" {
		t.Fatalf("unexpected response or RPC request: response=%+v request=%+v", response, rpc.request)
	}
}

func TestListAPIsHandlerRejectsInvalidJSON(t *testing.T) {
	setAPIResponseHandlers(t)
	rpc := &apiHandlerSystemRPCStub{}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/list",
		strings.NewReader(`{"keyword":`),
	)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	ListAPIsHandler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest || rpc.request != nil {
		t.Fatalf(
			"unexpected invalid JSON response: status=%d body=%s request=%+v",
			recorder.Code,
			recorder.Body.String(),
			rpc.request,
		)
	}
}

func setAPIResponseHandlers(t *testing.T) {
	t.Helper()
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	t.Cleanup(func() {
		httpx.SetOkHandler(nil)
		httpx.SetErrorHandlerCtx(nil)
	})
}

var _ systemclient.System = (*apiHandlerSystemRPCStub)(nil)
