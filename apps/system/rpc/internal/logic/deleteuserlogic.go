package logic

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteUserLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteUserLogic {
	return &DeleteUserLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteUserLogic) DeleteUser(in *system.DeleteUserRequest) (*system.EmptyResponse, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid delete user request")
	}
	if _, err := managedUser(l.ctx, l.svcCtx, in.OperatorId, in.Id, true); err != nil {
		return nil, err
	}
	if err := l.svcCtx.Sessions.RevokeAll(l.ctx, in.Id); err != nil {
		return nil, fmt.Errorf("revoke sessions before deleting user: %w", err)
	}
	if err := l.svcCtx.UserRepo.Delete(l.ctx, in.Id); err != nil {
		return nil, userManagementError(err)
	}
	return &system.EmptyResponse{}, nil
}
