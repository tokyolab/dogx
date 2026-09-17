package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateMenuLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateMenuLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateMenuLogic {
	return &UpdateMenuLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateMenuLogic) UpdateMenu(in *system.UpdateMenuRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidMenuRequest()
	}
	menu, err := normalizeMenuInput(in.Menu)
	if err != nil {
		return nil, err
	}
	if l.svcCtx.MenuRepo == nil {
		return nil, errors.New("menu repository is unavailable")
	}
	if err = l.svcCtx.MenuRepo.Update(l.ctx, in.Id, menu); err != nil {
		return nil, menuBusinessError(err)
	}
	return &system.EmptyResponse{}, nil
}
