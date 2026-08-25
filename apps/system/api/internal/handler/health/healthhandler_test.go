package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	commonresponse "github.com/tokyolab/dogx/pkg/response"

	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
)

type handlerSystemRPCStub struct {
	systemclient.System
	response *systemclient.ReadyResponse
	err      error
}

type handlerRedisPingerStub struct {
	ready bool
}

type handlerReadinessStub struct{}

func (handlerReadinessStub) Check(context.Context) error {
	return nil
}

func (s handlerRedisPingerStub) PingCtx(context.Context) bool {
	return s.ready
}

func (s handlerSystemRPCStub) CheckReady(
	context.Context,
	*systemclient.ReadyRequest,
	...grpc.CallOption,
) (*systemclient.ReadyResponse, error) {
	return s.response, s.err
}

func TestHealthHandler(t *testing.T) {
	setResponseHandlers(t)

	svcCtx := &svc.ServiceContext{Config: config.Config{App: config.AppConf{Version: "v0.1.0"}}}
	svcCtx.Config.Name = "system-api"

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder := httptest.NewRecorder()
	HealthHandler(svcCtx).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}

	var response struct {
		Code    uint32           `json:"code"`
		Message string           `json:"message"`
		Data    types.HealthResp `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != commonresponse.SuccessCode || response.Data.Status != "ok" || response.Data.Service != "system-api" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestReadyHandlerReturns503WithoutLeakingCause(t *testing.T) {
	setResponseHandlers(t)

	svcCtx := &svc.ServiceContext{
		Config:                 config.Config{App: config.AppConf{ReadinessTimeout: time.Second}},
		Redis:                  handlerRedisPingerStub{ready: true},
		AuthorizationReadiness: handlerReadinessStub{},
		SystemRpc: handlerSystemRPCStub{
			err: errors.New("private dependency detail"),
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recorder := httptest.NewRecorder()

	ReadyHandler(svcCtx).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "private dependency detail") {
		t.Fatalf("response leaked internal failure: %s", recorder.Body.String())
	}
}

func TestReadyHandler(t *testing.T) {
	setResponseHandlers(t)

	svcCtx := &svc.ServiceContext{
		Config:                 config.Config{App: config.AppConf{ReadinessTimeout: time.Second}},
		Redis:                  handlerRedisPingerStub{ready: true},
		AuthorizationReadiness: handlerReadinessStub{},
		SystemRpc: handlerSystemRPCStub{
			response: &systemclient.ReadyResponse{Status: "ready"},
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/ready", nil)
	recorder := httptest.NewRecorder()

	ReadyHandler(svcCtx).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", recorder.Code)
	}

	var response struct {
		Code uint32          `json:"code"`
		Data types.ReadyResp `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != commonresponse.SuccessCode || response.Data.Status != "ready" {
		t.Fatalf("unexpected response: %+v", response)
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
