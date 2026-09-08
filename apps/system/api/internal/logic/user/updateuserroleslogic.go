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

type UpdateUserRolesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateUserRolesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateUserRolesLogic {
	return &UpdateUserRolesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateUserRolesLogic) UpdateUserRoles(req *types.UpdateUserRolesReq) (resp *types.EmptyResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	id, err := operatorID(l.ctx)
	if err != nil {
		return nil, err
	}
	_, err = l.svcCtx.SystemRpc.ReplaceUserRoles(l.ctx, &systemclient.ReplaceUserRolesRequest{Id: req.Id, RoleIds: req.RoleIds, OperatorId: id})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
