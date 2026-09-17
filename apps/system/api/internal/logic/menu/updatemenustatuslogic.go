// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package menu

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateMenuStatusLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateMenuStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateMenuStatusLogic {
	return &UpdateMenuStatusLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateMenuStatusLogic) UpdateMenuStatus(req *types.UpdateMenuStatusReq) (resp *types.EmptyResp, err error) {
	if req == nil || req.Id <= 0 {
		return nil, invalidMenuRequest()
	}
	_, err = l.svcCtx.SystemRpc.UpdateMenuStatus(l.ctx, &systemclient.UpdateMenuStatusRequest{Id: req.Id, Status: req.Status})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
