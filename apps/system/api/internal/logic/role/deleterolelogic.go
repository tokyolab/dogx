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

type DeleteRoleLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Delete a role
func NewDeleteRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteRoleLogic {
	return &DeleteRoleLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *DeleteRoleLogic) DeleteRole(req *types.IDReq) (resp *types.EmptyResp, err error) {
	if req == nil || req.Id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid delete role request")
	}
	if _, err := l.svcCtx.SystemRpc.DeleteRole(l.ctx, &systemclient.DeleteRoleRequest{Id: req.Id}); err != nil {
		return nil, err
	}

	return &types.EmptyResp{}, nil
}
