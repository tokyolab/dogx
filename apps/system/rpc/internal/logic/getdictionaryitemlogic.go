package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetDictionaryItemLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetDictionaryItemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDictionaryItemLogic {
	return &GetDictionaryItemLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetDictionaryItemLogic) GetDictionaryItem(in *system.GetDictionaryItemRequest) (*system.GetDictionaryItemResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidDictionaryRequest()
	}
	item, err := l.svcCtx.DictionaryItemRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, dictionaryError(err)
	}
	return &system.GetDictionaryItemResponse{Item: toDictionaryItemInfo(*item)}, nil
}
