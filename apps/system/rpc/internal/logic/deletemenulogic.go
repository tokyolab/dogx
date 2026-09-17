package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteMenuLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteMenuLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteMenuLogic {
	return &DeleteMenuLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteMenuLogic) DeleteMenu(in *system.DeleteMenuRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidMenuRequest()
	}
	if l.svcCtx.MenuRepo == nil {
		return nil, errors.New("menu repository is unavailable")
	}
	if err := l.svcCtx.MenuRepo.Delete(l.ctx, in.Id); err != nil {
		return nil, menuBusinessError(err)
	}
	return &system.EmptyResponse{}, nil
}
