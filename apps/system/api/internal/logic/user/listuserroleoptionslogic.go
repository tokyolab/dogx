// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package user

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListUserRoleOptionsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListUserRoleOptionsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListUserRoleOptionsLogic {
	return &ListUserRoleOptionsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListUserRoleOptionsLogic) ListUserRoleOptions(req *types.UserRoleOptionsReq) (resp *types.UserRoleOptionsResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	result, err := l.svcCtx.SystemRpc.ListUserRoleOptions(l.ctx, &systemclient.ListRolesRequest{Page: req.Page, PageSize: req.PageSize, Keyword: req.Keyword})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned no role options")
	}
	items := make([]types.UserRoleItem, 0, len(result.Items))
	for _, role := range result.Items {
		items = append(items, toUserRoleItem(role))
	}
	return &types.UserRoleOptionsResp{Items: items, Total: result.Total}, nil
}
