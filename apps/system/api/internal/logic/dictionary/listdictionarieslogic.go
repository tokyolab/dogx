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

type ListDictionariesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListDictionariesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListDictionariesLogic {
	return &ListDictionariesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListDictionariesLogic) ListDictionaries() (resp *types.ListDictionariesResp, err error) {
	result, err := l.svcCtx.SystemRpc.ListDictionaries(l.ctx, &systemclient.ListDictionariesRequest{})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned an empty dictionary response")
	}
	resp = &types.ListDictionariesResp{Items: make([]types.DictionaryInfo, 0, len(result.Items))}
	for _, item := range result.Items {
		if item != nil {
			resp.Items = append(resp.Items, *toDictionaryInfo(item))
		}
	}
	return resp, nil
}
