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

type UpdateMenuLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateMenuLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateMenuLogic {
	return &UpdateMenuLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateMenuLogic) UpdateMenu(req *types.UpdateMenuReq) (resp *types.EmptyResp, err error) {
	if req == nil || req.Id <= 0 {
		return nil, invalidMenuRequest()
	}
	_, err = l.svcCtx.SystemRpc.UpdateMenu(l.ctx, &systemclient.UpdateMenuRequest{Id: req.Id, Menu: toMenuFields(req.MenuFields)})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
