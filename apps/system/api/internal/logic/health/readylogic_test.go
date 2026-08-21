package health

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/common/httperror"
	"google.golang.org/grpc"
)

type systemRPCStub struct {
	response *systemclient.ReadyResponse
	err      error
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
	}

	_, err := NewReadyLogic(context.Background(), svcCtx).Ready()
	if err == nil {
		t.Fatal("expected readiness error")
	}

	var apiErr *httperror.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected typed HTTP error, got: %T", err)
	}
}
