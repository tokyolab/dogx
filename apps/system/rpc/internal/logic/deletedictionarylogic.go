package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteDictionaryLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteDictionaryLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteDictionaryLogic {
	return &DeleteDictionaryLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteDictionaryLogic) DeleteDictionary(in *system.DeleteDictionaryRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidDictionaryRequest()
	}
	current, err := l.svcCtx.DictionaryRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, dictionaryError(err)
	}
	if err := l.svcCtx.DictionaryRepo.Delete(l.ctx, in.Id); err != nil {
		return nil, dictionaryError(err)
	}
	if err := invalidateDictionary(l.ctx, l.svcCtx, current.Code); err != nil {
		return nil, err
	}
	return &system.EmptyResponse{}, nil
}
