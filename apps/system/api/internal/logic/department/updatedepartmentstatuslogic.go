// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package department

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateDepartmentStatusLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateDepartmentStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateDepartmentStatusLogic {
	return &UpdateDepartmentStatusLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateDepartmentStatusLogic) UpdateDepartmentStatus(req *types.UpdateDepartmentStatusReq) (resp *types.EmptyResp, err error) {
	if req == nil {
		return nil, invalidDepartmentRequest()
	}
	_, err = l.svcCtx.SystemRpc.UpdateDepartmentStatus(l.ctx, &systemclient.UpdateDepartmentStatusRequest{Id: req.Id, Status: req.Status})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
