// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package role

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/menu"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetRoleMenusLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Get menu resources and authorizations assigned to a role
func NewGetRoleMenusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetRoleMenusLogic {
	return &GetRoleMenusLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetRoleMenusLogic) GetRoleMenus(req *types.GetRoleMenusReq) (resp *types.GetRoleMenusResp, err error) {
	if req == nil || req.RoleId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid role menu request")
	}
	result, err := l.svcCtx.SystemRpc.GetRoleMenus(l.ctx, &systemclient.GetRoleMenusRequest{RoleId: req.RoleId})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "invalid role menu response")
	}
	items := make([]types.MenuItem, 0, len(result.Items))
	for _, item := range result.Items {
		mapped, err := menu.ToMenuItem(item)
		if err != nil {
			return nil, err
		}
		items = append(items, *mapped)
	}
	return &types.GetRoleMenusResp{Items: items, MenuIds: append([]int64{}, result.MenuIds...)}, nil
}
