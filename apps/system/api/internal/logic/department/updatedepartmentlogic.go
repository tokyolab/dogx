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

type UpdateDepartmentLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateDepartmentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateDepartmentLogic {
	return &UpdateDepartmentLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateDepartmentLogic) UpdateDepartment(req *types.UpdateDepartmentReq) (resp *types.EmptyResp, err error) {
	if req == nil {
		return nil, invalidDepartmentRequest()
	}
	_, err = l.svcCtx.SystemRpc.UpdateDepartment(l.ctx, &systemclient.UpdateDepartmentRequest{Id: req.Id, ParentId: req.ParentId, Name: req.Name, Sort: req.Sort, Remark: req.Remark})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
