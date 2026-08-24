package logic

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChangePasswordRevokesSessionsBeforeUpdatingHash(t *testing.T) {
	repo := &userRepositoryStub{user: enabledUser()}
	passwords := &passwordVerifierStub{nextHash: "new-encoded-hash"}
	sessions := &sessionStoreLogicStub{}
	logic := NewChangePasswordLogic(context.Background(), &svc.ServiceContext{
		UserRepo:  repo,
		Passwords: passwords,
		Sessions:  sessions,
	})

	response, err := logic.ChangePassword(&system.ChangePasswordRequest{
		UserId:          42,
		CurrentPassword: "current-password",
		NewPassword:     "new-secure-password",
	})
	if err != nil {
		t.Fatalf("change password: %v", err)
	}
	if response == nil || sessions.revokedUserID != 42 || repo.passwordHash != "new-encoded-hash" {
		t.Fatalf("unexpected password change: response=%+v revoked=%d hash=%q", response, sessions.revokedUserID, repo.passwordHash)
	}
}

func TestChangePasswordRejectsWrongAndReusedPassword(t *testing.T) {
	wrongRepo := &userRepositoryStub{user: enabledUser()}
	wrongSessions := &sessionStoreLogicStub{}
	wrong := NewChangePasswordLogic(context.Background(), &svc.ServiceContext{
		UserRepo:  wrongRepo,
		Passwords: &passwordVerifierStub{err: authn.ErrPasswordMismatch},
		Sessions:  wrongSessions,
	})
	_, err := wrong.ChangePassword(&system.ChangePasswordRequest{
		UserId:          42,
		CurrentPassword: "wrong-password",
		NewPassword:     "new-secure-password",
	})
	if businessErr, ok := bizerror.From(err); !ok || businessErr.Error() != "当前密码错误" {
		t.Fatalf("unexpected wrong-password error: %v", err)
	}
	if wrongSessions.revokedUserID != 0 || wrongRepo.passwordHash != "" {
		t.Fatal("wrong password changed state")
	}

	reused := NewChangePasswordLogic(context.Background(), &svc.ServiceContext{})
	_, err = reused.ChangePassword(&system.ChangePasswordRequest{
		UserId:          42,
		CurrentPassword: "same-secure-password",
		NewPassword:     "same-secure-password",
	})
	if businessErr, ok := bizerror.From(err); !ok || businessErr.Error() != "新密码不能与当前密码相同" {
		t.Fatalf("unexpected reused-password error: %v", err)
	}
}

func TestChangePasswordValidatesInput(t *testing.T) {
	logic := NewChangePasswordLogic(context.Background(), &svc.ServiceContext{})
	requests := []*system.ChangePasswordRequest{
		nil,
		{},
		{UserId: 42, CurrentPassword: "current-password", NewPassword: "short"},
	}
	for _, request := range requests {
		if _, err := logic.ChangePassword(request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected invalid argument, got: %v", err)
		}
	}
}

func TestChangePasswordRevokesMissingAndDisabledUsers(t *testing.T) {
	missingSessions := &sessionStoreLogicStub{}
	missing := NewChangePasswordLogic(context.Background(), &svc.ServiceContext{
		UserRepo: &userRepositoryStub{findByIDErr: repository.ErrUserNotFound},
		Sessions: missingSessions,
	})
	_, err := missing.ChangePassword(&system.ChangePasswordRequest{
		UserId:          42,
		CurrentPassword: "current-password",
		NewPassword:     "new-secure-password",
	})
	if status.Code(err) != codes.Unauthenticated || missingSessions.revokedUserID != 42 {
		t.Fatalf("missing user was not revoked: err=%v revoked=%d", err, missingSessions.revokedUserID)
	}

	disabledUser := enabledUser()
	disabledUser.Status = model.RecordStatusDisabled
	disabledSessions := &sessionStoreLogicStub{}
	disabled := NewChangePasswordLogic(context.Background(), &svc.ServiceContext{
		UserRepo: &userRepositoryStub{user: disabledUser},
		Sessions: disabledSessions,
	})
	_, err = disabled.ChangePassword(&system.ChangePasswordRequest{
		UserId:          42,
		CurrentPassword: "current-password",
		NewPassword:     "new-secure-password",
	})
	if status.Code(err) != codes.Unauthenticated || disabledSessions.revokedUserID != 42 {
		t.Fatalf("disabled user was not revoked: err=%v revoked=%d", err, disabledSessions.revokedUserID)
	}
}

func TestChangePasswordDoesNotUpdateHashWhenRevocationFails(t *testing.T) {
	revokeErr := errors.New("redis unavailable")
	repo := &userRepositoryStub{user: enabledUser()}
	logic := NewChangePasswordLogic(context.Background(), &svc.ServiceContext{
		UserRepo:  repo,
		Passwords: &passwordVerifierStub{nextHash: "new-encoded-hash"},
		Sessions:  &sessionStoreLogicStub{revokeAllErr: revokeErr},
	})

	_, err := logic.ChangePassword(&system.ChangePasswordRequest{
		UserId:          42,
		CurrentPassword: "current-password",
		NewPassword:     "new-secure-password",
	})
	if !errors.Is(err, revokeErr) || repo.passwordHash != "" {
		t.Fatalf("unexpected revocation failure: err=%v hash=%q", err, repo.passwordHash)
	}
}

func TestChangePasswordReportsUpdateFailureAfterRevocation(t *testing.T) {
	updateErr := errors.New("postgres unavailable")
	repo := &userRepositoryStub{user: enabledUser(), updateErr: updateErr}
	sessions := &sessionStoreLogicStub{}
	logic := NewChangePasswordLogic(context.Background(), &svc.ServiceContext{
		UserRepo:  repo,
		Passwords: &passwordVerifierStub{nextHash: "new-encoded-hash"},
		Sessions:  sessions,
	})

	_, err := logic.ChangePassword(&system.ChangePasswordRequest{
		UserId:          42,
		CurrentPassword: "current-password",
		NewPassword:     "new-secure-password",
	})
	if !errors.Is(err, updateErr) || sessions.revokedUserID != 42 {
		t.Fatalf("unexpected update failure: err=%v revoked=%d", err, sessions.revokedUserID)
	}
}
