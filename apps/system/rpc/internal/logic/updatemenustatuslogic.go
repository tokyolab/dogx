package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateMenuStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateMenuStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateMenuStatusLogic {
	return &UpdateMenuStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateMenuStatusLogic) UpdateMenuStatus(in *system.UpdateMenuStatusRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || !validRecordStatus(in.Status) {
		return nil, invalidMenuRequest()
	}
	if l.svcCtx.MenuRepo == nil {
		return nil, errors.New("menu repository is unavailable")
	}
	if err := l.svcCtx.MenuRepo.UpdateStatus(l.ctx, in.Id, model.RecordStatus(in.Status)); err != nil {
		return nil, menuBusinessError(err)
	}
	return &system.EmptyResponse{}, nil
}
