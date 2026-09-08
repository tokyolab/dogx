package logic

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserLogic {
	return &GetUserLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetUserLogic) GetUser(in *system.GetUserRequest) (*system.GetUserResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}
	record, err := l.svcCtx.UserRepo.FindWithRoles(l.ctx, in.Id)
	if err != nil {
		return nil, userManagementError(err)
	}
	if record == nil {
		return nil, status.Error(codes.Internal, "user repository returned no user")
	}
	return &system.GetUserResponse{User: toUserInfo(*record)}, nil
}
