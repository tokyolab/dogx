package health

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/response"
	"google.golang.org/grpc"
)

type systemRPCStub struct {
	systemclient.System
	response *systemclient.ReadyResponse
	err      error
}

type redisPingerStub struct {
	ready bool
}

func (s redisPingerStub) PingCtx(context.Context) bool {
	return s.ready
}

func (s systemRPCStub) CheckReady(
	context.Context,
	*systemclient.ReadyRequest,
	...grpc.CallOption,
) (*systemclient.ReadyResponse, error) {
	return s.response, s.err
}

func TestReady(t *testing.T) {
	svcCtx := &svc.ServiceContext{
		Config: config.Config{App: config.AppConf{ReadinessTimeout: time.Second}},
		SystemRpc: systemRPCStub{
			response: &systemclient.ReadyResponse{Status: "ready"},
		},
		Redis: redisPingerStub{ready: true},
	}

	response, err := NewReadyLogic(context.Background(), svcCtx).Ready()
	if err != nil {
		t.Fatalf("ready returned error: %v", err)
	}
	if response.Status != "ready" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestReadyReturnsTypedUnavailableError(t *testing.T) {
	svcCtx := &svc.ServiceContext{
		Config:    config.Config{App: config.AppConf{ReadinessTimeout: time.Second}},
		SystemRpc: systemRPCStub{err: errors.New("system RPC unavailable")},
		Redis:     redisPingerStub{ready: true},
	}

	_, err := NewReadyLogic(context.Background(), svcCtx).Ready()
	if err == nil {
		t.Fatal("expected readiness error")
	}

	var apiErr *response.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected typed HTTP error, got: %T", err)
	}
}

func TestReadyReturnsUnavailableWhenAPIRedisIsDown(t *testing.T) {
	svcCtx := &svc.ServiceContext{
		Config: config.Config{App: config.AppConf{ReadinessTimeout: time.Second}},
		Redis:  redisPingerStub{ready: false},
	}

	_, err := NewReadyLogic(context.Background(), svcCtx).Ready()
	if err == nil {
		t.Fatal("expected API Redis readiness error")
	}
	var apiErr *response.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected typed HTTP error, got: %T", err)
	}
}
