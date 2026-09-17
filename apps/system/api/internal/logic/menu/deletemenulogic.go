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

type DeleteMenuLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewDeleteMenuLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteMenuLogic {
	return &DeleteMenuLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *DeleteMenuLogic) DeleteMenu(req *types.IDReq) (resp *types.EmptyResp, err error) {
	if req == nil || req.Id <= 0 {
		return nil, invalidMenuRequest()
	}
	_, err = l.svcCtx.SystemRpc.DeleteMenu(l.ctx, &systemclient.DeleteMenuRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
