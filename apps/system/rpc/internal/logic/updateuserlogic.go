package logic

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateUserLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateUserLogic {
	return &UpdateUserLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateUserLogic) UpdateUser(in *system.UpdateUserRequest) (*system.EmptyResponse, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid update user request")
	}
	profile, err := normalizeUserProfile(in.Nickname, in.Email, in.Phone, in.Remark)
	if err != nil {
		return nil, err
	}
	if _, err := managedUser(l.ctx, l.svcCtx, in.OperatorId, in.Id, false); err != nil {
		return nil, err
	}
	if err := l.svcCtx.UserRepo.UpdateProfile(l.ctx, in.Id, profile); err != nil {
		return nil, userManagementError(err)
	}
	return &system.EmptyResponse{}, nil
}
