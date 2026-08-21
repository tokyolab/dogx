package logic

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readinessStub struct {
	err error
}

func (s readinessStub) Check(context.Context) error {
	return s.err
}

func TestCheckReady(t *testing.T) {
	svcCtx := &svc.ServiceContext{
		Config:    config.Config{App: config.AppConf{ReadinessTimeout: time.Second}},
		Readiness: readinessStub{},
	}

	response, err := NewCheckReadyLogic(context.Background(), svcCtx).CheckReady(&system.ReadyRequest{})
	if err != nil {
		t.Fatalf("check ready returned error: %v", err)
	}
	if response.Status != "ready" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestCheckReadyReturnsUnavailableWithoutLeakingCause(t *testing.T) {
	svcCtx := &svc.ServiceContext{
		Config: config.Config{App: config.AppConf{ReadinessTimeout: time.Second}},
		Readiness: readinessStub{
			err: errors.New("private database address"),
		},
	}

	_, err := NewCheckReadyLogic(context.Background(), svcCtx).CheckReady(&system.ReadyRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("unexpected status code: %s", status.Code(err))
	}
	if strings.Contains(err.Error(), "private database address") {
		t.Fatalf("RPC error leaked dependency detail: %v", err)
	}
}
