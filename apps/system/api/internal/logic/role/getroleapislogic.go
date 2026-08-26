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

type GetRoleAPIsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Get API authorizations assigned to a role
func NewGetRoleAPIsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetRoleAPIsLogic {
	return &GetRoleAPIsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetRoleAPIsLogic) GetRoleAPIs(req *types.GetRoleAPIsReq) (resp *types.GetRoleAPIsResp, err error) {
	if req == nil || req.RoleId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid role API request")
	}
	result, err := l.svcCtx.SystemRpc.GetRoleAPIs(l.ctx, &systemclient.GetRoleAPIsRequest{
		RoleId: req.RoleId,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("system RPC returned empty role API authorizations")
	}

	apiIDs := make([]int64, len(result.ApiIds))
	copy(apiIDs, result.ApiIds)
	return &types.GetRoleAPIsResp{ApiIds: apiIDs}, nil
}
