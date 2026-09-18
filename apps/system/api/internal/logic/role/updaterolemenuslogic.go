// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package role

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UpdateRoleMenusLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Replace the complete PC menu authorization set for a role
func NewUpdateRoleMenusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateRoleMenusLogic {
	return &UpdateRoleMenusLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateRoleMenusLogic) UpdateRoleMenus(req *types.UpdateRoleMenusReq) (resp *types.EmptyResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid role menu request")
	}
	_, err = l.svcCtx.SystemRpc.ReplaceRoleMenus(l.ctx, &systemclient.ReplaceRoleMenusRequest{RoleId: req.RoleId, MenuIds: req.MenuIds})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
