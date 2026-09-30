package security

import (
	"context"
	"errors"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestSecurityLogic(t *testing.T) {
	stub := &systemRPCStub{cfg: &systemclient.LoginSecurityConfig{RateLimitWindowSeconds: 60, RateLimitMaxRequests: 30, FailureWindowSeconds: 900, FailureThreshold: 5, LockDurationSeconds: 900}}
	sc := &svc.ServiceContext{SystemRpc: stub}
	ctx := context.Background()
	get := NewGetLoginSecurityLogic(ctx, sc)
	update := NewUpdateLoginSecurityLogic(ctx, sc)
	cfg, err := get.GetLoginSecurity()
	if err != nil || cfg.RateLimitEnabled || cfg.FailureLockEnabled || cfg.FailureThreshold != 5 {
		t.Fatalf("%+v %v", cfg, err)
	}
	if _, err := update.UpdateLoginSecurity(cfg); err != nil || stub.update.RateLimitEnabled || stub.update.FailureLockEnabled || stub.update.RateLimitWindowSeconds != 60 || stub.update.RateLimitMaxRequests != 30 || stub.update.FailureWindowSeconds != 900 || stub.update.FailureThreshold != 5 || stub.update.LockDurationSeconds != 900 {
		t.Fatalf("%+v %v", stub.update, err)
	}
	if _, err := update.UpdateLoginSecurity(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	stub.cfg = nil
	if _, err := get.GetLoginSecurity(); status.Code(err) != codes.Internal {
		t.Fatal(err)
	}
	stub.err = errors.New("rpc offline")
	if _, err := get.GetLoginSecurity(); !errors.Is(err, stub.err) {
		t.Fatal(err)
	}
	if _, err := update.UpdateLoginSecurity(&types.LoginSecurityConfig{}); !errors.Is(err, stub.err) {
		t.Fatal(err)
	}
}
