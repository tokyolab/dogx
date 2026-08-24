package logic

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ChangePasswordLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewChangePasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ChangePasswordLogic {
	return &ChangePasswordLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ChangePasswordLogic) ChangePassword(in *system.ChangePasswordRequest) (*system.EmptyResponse, error) {
	if in == nil || in.UserId <= 0 || in.CurrentPassword == "" ||
		len(in.CurrentPassword) > authn.MaxPasswordBytes {
		return nil, status.Error(codes.InvalidArgument, "invalid change password request")
	}
	if err := authn.ValidatePassword(in.NewPassword); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid new password")
	}
	if in.CurrentPassword == in.NewPassword {
		return nil, bizerror.New("新密码不能与当前密码相同")
	}

	user, err := l.svcCtx.UserRepo.FindByID(l.ctx, in.UserId)
	if errors.Is(err, repository.ErrUserNotFound) {
		_ = l.svcCtx.Sessions.RevokeAll(l.ctx, in.UserId)
		return nil, status.Error(codes.Unauthenticated, "user no longer exists")
	}
	if err != nil {
		return nil, fmt.Errorf("find password owner: %w", err)
	}
	if user.Status != model.RecordStatusEnabled {
		if err := l.svcCtx.Sessions.RevokeAll(l.ctx, user.ID); err != nil {
			return nil, fmt.Errorf("revoke disabled user sessions: %w", err)
		}
		return nil, status.Error(codes.Unauthenticated, "user is disabled")
	}
	if err := l.svcCtx.Passwords.Verify(user.PasswordHash, in.CurrentPassword); err != nil {
		if errors.Is(err, authn.ErrPasswordMismatch) {
			return nil, bizerror.New("当前密码错误")
		}
		return nil, fmt.Errorf("verify current password: %w", err)
	}

	passwordHash, err := l.svcCtx.Passwords.Hash(in.NewPassword)
	if err != nil {
		return nil, fmt.Errorf("hash new password: %w", err)
	}

	// Revoke before persisting the new hash. If PostgreSQL then fails, the user is
	// logged out but can still retry with the old password; old sessions never
	// remain valid after a successful password change.
	if err := l.svcCtx.Sessions.RevokeAll(l.ctx, user.ID); err != nil {
		return nil, fmt.Errorf("revoke sessions before password change: %w", err)
	}
	if err := l.svcCtx.UserRepo.UpdatePasswordHash(l.ctx, user.ID, passwordHash); err != nil {
		return nil, fmt.Errorf("update password hash: %w", err)
	}

	return &system.EmptyResponse{}, nil
}
