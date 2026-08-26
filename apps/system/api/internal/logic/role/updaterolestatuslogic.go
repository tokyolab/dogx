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

type UpdateRoleStatusLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Enable or disable a role
func NewUpdateRoleStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateRoleStatusLogic {
	return &UpdateRoleStatusLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateRoleStatusLogic) UpdateRoleStatus(req *types.UpdateRoleStatusReq) (resp *types.EmptyResp, err error) {
	if req == nil || req.Id <= 0 || (req.Status != 0 && req.Status != 1) {
		return nil, status.Error(codes.InvalidArgument, "invalid update role status request")
	}
	if _, err := l.svcCtx.SystemRpc.UpdateRoleStatus(l.ctx, &systemclient.UpdateRoleStatusRequest{
		Id:     req.Id,
		Status: int32(req.Status),
	}); err != nil {
		return nil, err
	}

	return &types.EmptyResp{}, nil
}
