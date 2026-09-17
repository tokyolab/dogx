package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetMenuLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetMenuLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetMenuLogic {
	return &GetMenuLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetMenuLogic) GetMenu(in *system.GetMenuRequest) (*system.GetMenuResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidMenuRequest()
	}
	if l.svcCtx.MenuRepo == nil {
		return nil, errors.New("menu repository is unavailable")
	}
	menu, err := l.svcCtx.MenuRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, menuBusinessError(err)
	}
	if menu == nil {
		return nil, errors.New("menu repository returned nil menu")
	}
	return &system.GetMenuResponse{Menu: toMenuInfo(*menu)}, nil
}
