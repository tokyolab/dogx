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

type GetDictionaryItemLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetDictionaryItemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDictionaryItemLogic {
	return &GetDictionaryItemLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetDictionaryItemLogic) GetDictionaryItem(req *types.GetDictionaryItemReq) (resp *types.GetDictionaryItemResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dictionary request")
	}
	result, err := l.svcCtx.SystemRpc.GetDictionaryItem(l.ctx, &systemclient.GetDictionaryItemRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned an empty dictionary response")
	}
	if result.Item == nil {
		return nil, status.Error(codes.Internal, "system RPC returned empty dictionary detail")
	}
	return &types.GetDictionaryItemResp{Item: *toDictionaryItemInfo(result.Item)}, nil
}
