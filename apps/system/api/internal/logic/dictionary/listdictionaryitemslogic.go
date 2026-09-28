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

type ListDictionaryItemsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListDictionaryItemsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListDictionaryItemsLogic {
	return &ListDictionaryItemsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListDictionaryItemsLogic) ListDictionaryItems(req *types.ListDictionaryItemsReq) (resp *types.ListDictionaryItemsResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dictionary request")
	}
	result, err := l.svcCtx.SystemRpc.ListDictionaryItems(l.ctx, &systemclient.ListDictionaryItemsRequest{DictionaryId: req.DictionaryId})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned an empty dictionary response")
	}
	resp = &types.ListDictionaryItemsResp{Items: make([]types.DictionaryItemInfo, 0, len(result.Items))}
	for _, item := range result.Items {
		if item != nil {
			resp.Items = append(resp.Items, *toDictionaryItemInfo(item))
		}
	}
	return resp, nil
}
