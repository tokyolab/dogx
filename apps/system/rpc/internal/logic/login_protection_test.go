package logic

import (
	"context"
	"errors"
	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestLoginProtectionMatrix(t *testing.T) {
	failure := errors.New("offline")
	for _, tc := range []struct {
		name                                                         string
		disabled, locked, protectionOff                              bool
		repoErr, passwordErr, configErr, checkErr, failErr, clearErr error
		status                                                       codes.Code
		count, clear, audits                                         int
		skipLookup                                                   bool
	}{
		{name: "success", status: codes.OK, clear: 1, audits: 1},
		{name: "unknown username counts", repoErr: repository.ErrUserNotFound, status: codes.Code(bizerror.DefaultCode), count: 1, audits: 1},
		{name: "wrong password counts", passwordErr: authn.ErrPasswordMismatch, status: codes.Code(bizerror.DefaultCode), count: 1, audits: 1},
		{name: "disabled correct", disabled: true, status: codes.Code(bizerror.DefaultCode), audits: 1},
		{name: "disabled incorrect", disabled: true, passwordErr: authn.ErrPasswordMismatch, status: codes.Code(bizerror.DefaultCode), audits: 1},
		{name: "database failure", repoErr: failure, status: codes.Unknown, audits: 1},
		{name: "hash failure", passwordErr: failure, status: codes.Unknown, audits: 1},
		{name: "locked including admin", locked: true, status: codes.Code(bizerror.DefaultCode), skipLookup: true},
		{name: "config unavailable", configErr: failure, status: codes.Unavailable, skipLookup: true},
		{name: "redis check unavailable", checkErr: failure, status: codes.Unavailable, skipLookup: true},
		{name: "redis increment unavailable", passwordErr: authn.ErrPasswordMismatch, failErr: failure, status: codes.Unavailable, count: 1, audits: 1},
		{name: "redis clear unavailable", clearErr: failure, status: codes.Unavailable, clear: 1},
		{name: "off ignores old lock", protectionOff: true, locked: true, status: codes.OK, audits: 1},
		{name: "off no counting", protectionOff: true, passwordErr: authn.ErrPasswordMismatch, status: codes.Code(bizerror.DefaultCode), audits: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user := enabledUser()
			if tc.disabled {
				user.Status = model.RecordStatusDisabled
			}
			repo := &userRepositoryStub{user: user, findErr: tc.repoErr}
			pw := &passwordVerifierStub{err: tc.passwordErr}
			audit := &loginLogRepositoryStub{}
			store := &loginFailuresStub{locked: tc.locked, checkErr: tc.checkErr, failErr: tc.failErr, clearErr: tc.clearErr}
			tokens := &credentialIssuerStub{credentials: &authn.Credentials{AccessToken: "at"}}
			sc := &svc.ServiceContext{UserRepo: repo, Passwords: pw, LoginLogRepo: audit, Tokens: tokens, Security: &securityRuntimeStub{cfg: &system.LoginSecurityConfig{FailureLockEnabled: !tc.protectionOff}, err: tc.configErr}, LoginFailures: store}
			_, err := NewLoginLogic(context.Background(), sc).Login(&system.LoginRequest{Username: "Admin", Password: "WrongPass1!"})
			gotCode := status.Code(err)
			if business, ok := bizerror.From(err); ok {
				gotCode = codes.Code(business.Code())
			}
			if gotCode != tc.status {
				t.Fatalf("error %v", err)
			}
			if store.failures != tc.count || store.clears != tc.clear || len(audit.logs) != tc.audits {
				t.Fatalf("count=%d clear=%d audit=%d", store.failures, store.clears, len(audit.logs))
			}
			if tc.skipLookup && (repo.username != "" || pw.hash != "" || tokens.userID != 0) {
				t.Fatal("blocked attempt reached credential flow")
			}
			if tc.protectionOff && store.checks != 0 {
				t.Fatal("disabled lock checked Redis")
			}
		})
	}
	_, err := NewLoginLogic(context.Background(), &svc.ServiceContext{}).Login(&system.LoginRequest{Username: "Admin", Password: "Valid123!"})
	if status.Code(err) != codes.Unavailable {
		t.Fatal("nil protection not fail closed")
	}
}
