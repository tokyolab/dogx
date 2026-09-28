package logic

import (
	"context"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateDictionaryItemLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateDictionaryItemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateDictionaryItemLogic {
	return &UpdateDictionaryItemLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateDictionaryItemLogic) UpdateDictionaryItem(in *system.UpdateDictionaryItemRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || !validDictionaryFields(in.Label, in.Remark) || in.Sort < 0 {
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
	if err := l.svcCtx.DictionaryItemRepo.Update(l.ctx, in.Id, &model.DictionaryItem{Label: strings.TrimSpace(in.Label), Sort: in.Sort, Remark: strings.TrimSpace(in.Remark)}); err != nil {
		return nil, dictionaryError(err)
	}
	if err := invalidateDictionary(l.ctx, l.svcCtx, dictionary.Code); err != nil {
		return nil, err
	}
	return &system.EmptyResponse{}, nil
}
