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

type GetDictionaryLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetDictionaryLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetDictionaryLogic {
	return &GetDictionaryLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetDictionaryLogic) GetDictionary(req *types.GetDictionaryReq) (resp *types.GetDictionaryResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dictionary request")
	}
	result, err := l.svcCtx.SystemRpc.GetDictionary(l.ctx, &systemclient.GetDictionaryRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned an empty dictionary response")
	}
	if result.Dictionary == nil {
		return nil, status.Error(codes.Internal, "system RPC returned empty dictionary detail")
	}
	return &types.GetDictionaryResp{Dictionary: *toDictionaryInfo(result.Dictionary)}, nil
}
