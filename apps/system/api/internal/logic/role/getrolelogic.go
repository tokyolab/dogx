// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package role

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetRoleLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Get a role
func NewGetRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetRoleLogic {
	return &GetRoleLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetRoleLogic) GetRole(req *types.IDReq) (resp *types.RoleItem, err error) {
	if req == nil || req.Id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid role request")
	}
	result, err := l.svcCtx.SystemRpc.GetRole(l.ctx, &systemclient.GetRoleRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Role == nil {
		return nil, errors.New("system RPC returned an empty role")
	}

	return toRoleItem(result.Role), nil
}
