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

type DeleteDepartmentLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewDeleteDepartmentLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteDepartmentLogic {
	return &DeleteDepartmentLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *DeleteDepartmentLogic) DeleteDepartment(req *types.IDReq) (resp *types.EmptyResp, err error) {
	if req == nil || req.Id <= 0 {
		return nil, invalidDepartmentRequest()
	}
	_, err = l.svcCtx.SystemRpc.DeleteDepartment(l.ctx, &systemclient.DeleteDepartmentRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
