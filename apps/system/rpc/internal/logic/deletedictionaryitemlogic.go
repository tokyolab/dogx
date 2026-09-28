package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteDictionaryItemLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteDictionaryItemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteDictionaryItemLogic {
	return &DeleteDictionaryItemLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteDictionaryItemLogic) DeleteDictionaryItem(in *system.DeleteDictionaryItemRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidDictionaryRequest()
	}
	current, err := l.svcCtx.DictionaryItemRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, dictionaryError(err)
	}
	dictionary, err := l.svcCtx.DictionaryRepo.FindByID(l.ctx, current.DictionaryID)
	if err != nil {
		return nil, dictionaryError(err)
	}
	if err := l.svcCtx.DictionaryItemRepo.Delete(l.ctx, in.Id); err != nil {
		return nil, dictionaryError(err)
	}
	if err := invalidateDictionary(l.ctx, l.svcCtx, dictionary.Code); err != nil {
		return nil, err
	}
	return &system.EmptyResponse{}, nil
}
