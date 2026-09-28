package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListDictionaryItemsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListDictionaryItemsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListDictionaryItemsLogic {
	return &ListDictionaryItemsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListDictionaryItemsLogic) ListDictionaryItems(in *system.ListDictionaryItemsRequest) (*system.ListDictionaryItemsResponse, error) {
	if in == nil || in.DictionaryId <= 0 {
		return nil, invalidDictionaryRequest()
	}
	if _, err := l.svcCtx.DictionaryRepo.FindByID(l.ctx, in.DictionaryId); err != nil {
		return nil, dictionaryError(err)
	}
	items, err := l.svcCtx.DictionaryItemRepo.ListByDictionaryIDs(l.ctx, []int64{in.DictionaryId})
	if err != nil {
		return nil, dictionaryError(err)
	}
	result := &system.ListDictionaryItemsResponse{Items: make([]*system.DictionaryItemInfo, 0, len(items))}
	for _, item := range items {
		result.Items = append(result.Items, toDictionaryItemInfo(item))
	}
	return result, nil
}
