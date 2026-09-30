package loginprotection

import (
	"context"
	"errors"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"testing"
)

func validConfig() *system.LoginSecurityConfig {
	return &system.LoginSecurityConfig{RateLimitEnabled: true, RateLimitWindowSeconds: 60, RateLimitMaxRequests: 30, FailureLockEnabled: true, FailureWindowSeconds: 900, FailureThreshold: 5, LockDurationSeconds: 900}
}

func TestConfigValidation(t *testing.T) {
	if Validate(nil) == nil {
		t.Fatal("nil accepted")
	}
	cfg := validConfig()
	if err := Validate(cfg); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"rate_limit_window_seconds", "rate_limit_max_requests", "failure_window_seconds", "failure_threshold", "lock_duration_seconds"} {
		for _, value := range []int32{0, 100001} {
			c := proto.Clone(cfg).(*system.LoginSecurityConfig)
			field := c.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
			c.ProtoReflect().Set(field, protoreflect.ValueOfInt32(value))
			if Validate(c) == nil {
				t.Fatalf("accepted %s=%d", name, value)
			}
		}
	}
	cfg.RateLimitEnabled = false
	cfg.FailureLockEnabled = false
	if Validate(cfg) != nil {
		t.Fatal("false flags rejected")
	}
}

func TestRuntimeReloadAndSnapshotOwnership(t *testing.T) {
	cfg := validConfig()
	var loadErr error
	r := &Runtime{load: func(context.Context) (*system.LoginSecurityConfig, error) { return cfg, loadErr }}
	if _, err := r.Current(); err == nil {
		t.Fatal("missing config accepted")
	}
	if err := r.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg.FailureThreshold = 20
	snapshot, _ := r.Current()
	if snapshot.FailureThreshold != 5 {
		t.Fatal("loader owns stored memory")
	}
	snapshot.FailureThreshold = 40
	snapshot, _ = r.Current()
	if snapshot.FailureThreshold != 5 {
		t.Fatal("caller owns stored memory")
	}
	loadErr = errors.New("db offline")
	if r.Reload(context.Background()) == nil {
		t.Fatal("missing reload error")
	}
	if _, err := r.Current(); err == nil {
		t.Fatal("stale config usable after source failure")
	}
	loadErr = nil
	cfg.FailureThreshold = 0
	if r.Reload(context.Background()) == nil {
		t.Fatal("invalid config accepted")
	}
	cfg.FailureThreshold = 5
	if err := r.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
}
