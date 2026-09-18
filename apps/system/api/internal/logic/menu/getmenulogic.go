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

type GetMenuLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetMenuLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetMenuLogic {
	return &GetMenuLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetMenuLogic) GetMenu(req *types.IDReq) (resp *types.MenuItem, err error) {
	if req == nil || req.Id <= 0 {
		return nil, invalidMenuRequest()
	}
	result, err := l.svcCtx.SystemRpc.GetMenu(l.ctx, &systemclient.GetMenuRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidMenuResponse()
	}
	return ToMenuItem(result.Menu)
}
