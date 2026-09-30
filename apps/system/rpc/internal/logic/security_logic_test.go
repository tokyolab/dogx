package logic

import (
	"context"
	"errors"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestSecurityConfigurationReadAndSave(t *testing.T) {
	cfg := &system.LoginSecurityConfig{RateLimitWindowSeconds: 60, RateLimitMaxRequests: 30, FailureWindowSeconds: 900, FailureThreshold: 5, LockDurationSeconds: 900}
	failure := errors.New("offline")
	for _, tc := range []struct {
		name                          string
		repoErr, reloadErr, notifyErr error
		want                          codes.Code
		sync                          int
	}{
		{"false persists", nil, nil, nil, codes.OK, 1},
		{"db fails", failure, nil, nil, codes.Unknown, 0},
		{"reload fails but notify attempted", nil, failure, nil, codes.Code(bizerror.DefaultCode), 1},
		{"notify fails", nil, nil, failure, codes.Code(bizerror.DefaultCode), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &securityConfigRepositoryStub{err: tc.repoErr}
			runtime := &securityRuntimeStub{reloadErr: tc.reloadErr, notifyErr: tc.notifyErr}
			sc := &svc.ServiceContext{SecurityRepo: repo, Security: runtime}
			_, err := NewUpdateLoginSecurityLogic(context.Background(), sc).UpdateLoginSecurity(cfg)
			gotCode := status.Code(err)
			if business, ok := bizerror.From(err); ok {
				gotCode = codes.Code(business.Code())
				if business.Subcode() != "system.security.sync_failed" {
					t.Fatal(business.Subcode())
				}
			}
			if gotCode != tc.want || runtime.reloads != tc.sync || runtime.notifications != tc.sync {
				t.Fatalf("%v reload=%d notify=%d", err, runtime.reloads, runtime.notifications)
			}
			if repo.updated == nil || repo.updated.LoginRateLimitEnabled || repo.updated.LoginFailureLockEnabled || repo.updated.LoginFailureThreshold != 5 {
				t.Fatal("incorrect mapping")
			}
		})
	}
	if _, err := NewUpdateLoginSecurityLogic(context.Background(), &svc.ServiceContext{}).UpdateLoginSecurity(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	repo := &securityConfigRepositoryStub{cfg: &model.SecurityConfig{LoginRateLimitWindowSeconds: 60, LoginRateLimitMaxRequests: 30, LoginFailureWindowSeconds: 900, LoginFailureThreshold: 5, LoginLockDurationSeconds: 900}}
	logic := NewGetLoginSecurityLogic(context.Background(), &svc.ServiceContext{SecurityRepo: repo})
	got, err := logic.GetLoginSecurity(&system.GetLoginSecurityRequest{})
	if err != nil || got.RateLimitEnabled || got.FailureThreshold != 5 {
		t.Fatalf("%+v %v", got, err)
	}
	repo.err = failure
	if _, err = logic.GetLoginSecurity(&system.GetLoginSecurityRequest{}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	repo.err = nil
	repo.cfg.LoginFailureThreshold = 0
	if _, err = logic.GetLoginSecurity(&system.GetLoginSecurityRequest{}); err == nil {
		t.Fatal("invalid stored config accepted")
	}
}
