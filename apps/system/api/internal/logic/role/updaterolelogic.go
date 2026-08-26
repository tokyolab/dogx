// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package role

import (
	"context"
	"math"
	"strings"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UpdateRoleLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Update role metadata
func NewUpdateRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateRoleLogic {
	return &UpdateRoleLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateRoleLogic) UpdateRole(req *types.UpdateRoleReq) (resp *types.EmptyResp, err error) {
	if req == nil || req.Id <= 0 || strings.TrimSpace(req.Code) == "" ||
		strings.TrimSpace(req.Name) == "" || req.Sort < 0 || req.Sort > math.MaxInt32 {
		return nil, status.Error(codes.InvalidArgument, "invalid update role request")
	}
	if _, err := l.svcCtx.SystemRpc.UpdateRole(l.ctx, &systemclient.UpdateRoleRequest{
		Id:          req.Id,
		Code:        req.Code,
		Name:        req.Name,
		Description: req.Description,
		Sort:        int32(req.Sort),
	}); err != nil {
		return nil, err
	}

	return &types.EmptyResp{}, nil
}
