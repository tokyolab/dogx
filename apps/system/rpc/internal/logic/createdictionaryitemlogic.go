package logic

import (
	"context"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateDictionaryItemLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateDictionaryItemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateDictionaryItemLogic {
	return &CreateDictionaryItemLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateDictionaryItemLogic) CreateDictionaryItem(in *system.CreateDictionaryItemRequest) (*system.CreateDictionaryItemResponse, error) {
	if in == nil || in.DictionaryId <= 0 || !validDictionaryFields(in.Label, in.Remark) || !validDictionaryText(in.Value, 128) || in.Sort < 0 || !validRecordStatus(in.Status) {
		return nil, invalidDictionaryRequest()
	}
	dictionary, err := l.svcCtx.DictionaryRepo.FindByID(l.ctx, in.DictionaryId)
	if err != nil {
		return nil, dictionaryError(err)
	}
	item := &model.DictionaryItem{DictionaryID: in.DictionaryId, Label: strings.TrimSpace(in.Label), Value: strings.TrimSpace(in.Value), Sort: in.Sort, Remark: strings.TrimSpace(in.Remark), Status: model.RecordStatus(in.Status)}
	if err := l.svcCtx.DictionaryItemRepo.Create(l.ctx, item); err != nil {
		return nil, dictionaryError(err)
	}
	if err := invalidateDictionary(l.ctx, l.svcCtx, dictionary.Code); err != nil {
		return nil, err
	}
	return &system.CreateDictionaryItemResponse{Id: item.ID}, nil
}
