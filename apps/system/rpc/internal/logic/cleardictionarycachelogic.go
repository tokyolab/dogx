package logic

import (
	"context"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ClearDictionaryCacheLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewClearDictionaryCacheLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ClearDictionaryCacheLogic {
	return &ClearDictionaryCacheLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ClearDictionaryCacheLogic) ClearDictionaryCache(in *system.ClearDictionaryCacheRequest) (*system.EmptyResponse, error) {
	if err := l.svcCtx.DictionaryCache.Clear(l.ctx); err != nil {
		return nil, fmt.Errorf("clear dictionary cache: %w", err)
	}
	return &system.EmptyResponse{}, nil
}
