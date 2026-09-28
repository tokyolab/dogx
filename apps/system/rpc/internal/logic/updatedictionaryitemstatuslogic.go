package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateDictionaryItemStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateDictionaryItemStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateDictionaryItemStatusLogic {
	return &UpdateDictionaryItemStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateDictionaryItemStatusLogic) UpdateDictionaryItemStatus(in *system.UpdateDictionaryItemStatusRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || !validRecordStatus(in.Status) {
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
	if err := l.svcCtx.DictionaryItemRepo.UpdateStatus(l.ctx, in.Id, model.RecordStatus(in.Status)); err != nil {
		return nil, dictionaryError(err)
	}
	if err := invalidateDictionary(l.ctx, l.svcCtx, dictionary.Code); err != nil {
		return nil, err
	}
	return &system.EmptyResponse{}, nil
}
