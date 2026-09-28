// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package dictionary

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ClearDictionaryCacheLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewClearDictionaryCacheLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ClearDictionaryCacheLogic {
	return &ClearDictionaryCacheLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ClearDictionaryCacheLogic) ClearDictionaryCache() (resp *types.EmptyResp, err error) {
	result, err := l.svcCtx.SystemRpc.ClearDictionaryCache(l.ctx, &systemclient.ClearDictionaryCacheRequest{})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned an empty dictionary response")
	}
	return &types.EmptyResp{}, nil
}
