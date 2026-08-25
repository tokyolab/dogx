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

type UpdateRoleAPIsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Replace the complete API authorization set for a role
func NewUpdateRoleAPIsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateRoleAPIsLogic {
	return &UpdateRoleAPIsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateRoleAPIsLogic) UpdateRoleAPIs(req *types.UpdateRoleAPIsReq) (resp *types.EmptyResp, err error) {
	if req == nil || req.RoleId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid role API authorization request")
	}
	_, err = l.svcCtx.SystemRpc.ReplaceRoleAPIs(l.ctx, &systemclient.ReplaceRoleAPIsRequest{
		RoleId: req.RoleId,
		ApiIds: req.ApiIds,
	})
	if err != nil {
		return nil, err
	}

	return &types.EmptyResp{}, nil
}
