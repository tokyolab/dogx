package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetDictionaryLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetDictionaryLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDictionaryLogic {
	return &GetDictionaryLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetDictionaryLogic) GetDictionary(in *system.GetDictionaryRequest) (*system.GetDictionaryResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidDictionaryRequest()
	}
	item, err := l.svcCtx.DictionaryRepo.FindByID(l.ctx, in.Id)
	if err != nil {
		return nil, dictionaryError(err)
	}
	return &system.GetDictionaryResponse{Dictionary: toDictionaryInfo(*item)}, nil
}
