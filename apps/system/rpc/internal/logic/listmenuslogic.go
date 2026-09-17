package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListMenusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListMenusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListMenusLogic {
	return &ListMenusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListMenusLogic) ListMenus(in *system.ListMenusRequest) (*system.ListMenusResponse, error) {
	if in == nil {
		return nil, invalidMenuRequest()
	}
	if l.svcCtx.MenuRepo == nil {
		return nil, errors.New("menu repository is unavailable")
	}
	menus, err := l.svcCtx.MenuRepo.List(l.ctx)
	if err != nil {
		return nil, menuBusinessError(err)
	}
	items := make([]*system.MenuInfo, 0, len(menus))
	for _, menu := range menus {
		items = append(items, toMenuInfo(menu))
	}
	return &system.ListMenusResponse{Items: items}, nil
}
