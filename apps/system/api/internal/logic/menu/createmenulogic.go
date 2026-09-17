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

type CreateMenuLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateMenuLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateMenuLogic {
	return &CreateMenuLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateMenuLogic) CreateMenu(req *types.CreateMenuReq) (resp *types.CreateMenuResp, err error) {
	if req == nil {
		return nil, invalidMenuRequest()
	}
	result, err := l.svcCtx.SystemRpc.CreateMenu(l.ctx, &systemclient.CreateMenuRequest{Menu: toMenuFields(req.MenuFields), Status: req.Status})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Id <= 0 {
		return nil, invalidMenuResponse()
	}
	return &types.CreateMenuResp{Id: result.Id}, nil
}
