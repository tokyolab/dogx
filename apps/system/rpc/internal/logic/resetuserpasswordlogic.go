package logic

import (
	"context"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ResetUserPasswordLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewResetUserPasswordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ResetUserPasswordLogic {
	return &ResetUserPasswordLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ResetUserPasswordLogic) ResetUserPassword(in *system.ResetUserPasswordRequest) (*system.EmptyResponse, error) {
	if in == nil || authn.ValidatePassword(in.Password) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid reset password request")
	}
	if _, err := managedUser(l.ctx, l.svcCtx, in.OperatorId, in.Id, false); err != nil {
		return nil, err
	}
	hash, err := l.svcCtx.Passwords.Hash(in.Password)
	if err != nil {
		return nil, fmt.Errorf("hash reset user password: %w", err)
	}
	if err := l.svcCtx.Sessions.RevokeAll(l.ctx, in.Id); err != nil {
		return nil, fmt.Errorf("revoke sessions before resetting user password: %w", err)
	}
	if err := l.svcCtx.UserRepo.UpdatePasswordHash(l.ctx, in.Id, hash); err != nil {
		return nil, userManagementError(err)
	}
	return &system.EmptyResponse{}, nil
}
