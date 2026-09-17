package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateMenuLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateMenuLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateMenuLogic {
	return &CreateMenuLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateMenuLogic) CreateMenu(in *system.CreateMenuRequest) (*system.CreateMenuResponse, error) {
	if in == nil || !validRecordStatus(in.Status) {
		return nil, invalidMenuRequest()
	}
	menu, err := normalizeMenuInput(in.Menu)
	if err != nil {
		return nil, err
	}
	if l.svcCtx.MenuRepo == nil {
		return nil, errors.New("menu repository is unavailable")
	}
	menu.Status = model.RecordStatus(in.Status)
	if err = l.svcCtx.MenuRepo.Create(l.ctx, menu); err != nil {
		return nil, menuBusinessError(err)
	}
	return &system.CreateMenuResponse{Id: menu.ID}, nil
}
