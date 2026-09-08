package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListUserRoleOptionsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListUserRoleOptionsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListUserRoleOptionsLogic {
	return &ListUserRoleOptionsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListUserRoleOptionsLogic) ListUserRoleOptions(in *system.ListRolesRequest) (*system.ListUserRoleOptionsResponse, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid role options request")
	}
	offset, err := userPageOffset(in.Page, in.PageSize, in.Keyword)
	if err != nil {
		return nil, err
	}
	roles, total, err := l.svcCtx.RoleRepo.List(l.ctx, repository.RoleListQuery{Keyword: in.Keyword, Offset: offset, Limit: int(in.PageSize), AssignableOnly: true})
	if err != nil {
		return nil, userManagementError(err)
	}
	items := make([]*system.UserRoleInfo, 0, len(roles))
	for _, role := range roles {
		items = append(items, toUserRoleInfo(role))
	}
	return &system.ListUserRoleOptionsResponse{Items: items, Total: total}, nil
}
