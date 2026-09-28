package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListDictionariesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListDictionariesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListDictionariesLogic {
	return &ListDictionariesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListDictionariesLogic) ListDictionaries(in *system.ListDictionariesRequest) (*system.ListDictionariesResponse, error) {
	items, err := l.svcCtx.DictionaryRepo.List(l.ctx)
	if err != nil {
		return nil, dictionaryError(err)
	}
	result := &system.ListDictionariesResponse{Items: make([]*system.DictionaryInfo, 0, len(items))}
	for _, item := range items {
		result.Items = append(result.Items, toDictionaryInfo(item))
	}
	return result, nil
}
