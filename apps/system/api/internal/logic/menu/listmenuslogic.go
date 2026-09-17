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

type ListMenusLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListMenusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListMenusLogic {
	return &ListMenusLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListMenusLogic) ListMenus() (resp *types.MenuListResp, err error) {
	result, err := l.svcCtx.SystemRpc.ListMenus(l.ctx, &systemclient.ListMenusRequest{})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidMenuResponse()
	}
	items := make([]types.MenuItem, 0, len(result.Items))
	for _, item := range result.Items {
		mapped, mapErr := toMenuItem(item)
		if mapErr != nil {
			return nil, mapErr
		}
		items = append(items, *mapped)
	}
	return &types.MenuListResp{Items: items}, nil
}
