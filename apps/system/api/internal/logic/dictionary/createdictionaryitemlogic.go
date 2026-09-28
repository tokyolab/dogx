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

type CreateDictionaryItemLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateDictionaryItemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateDictionaryItemLogic {
	return &CreateDictionaryItemLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateDictionaryItemLogic) CreateDictionaryItem(req *types.CreateDictionaryItemReq) (resp *types.CreateDictionaryItemResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dictionary request")
	}
	result, err := l.svcCtx.SystemRpc.CreateDictionaryItem(l.ctx, &systemclient.CreateDictionaryItemRequest{DictionaryId: req.DictionaryId, Label: req.Label, Value: req.Value, Sort: req.Sort, Remark: req.Remark, Status: req.Status})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned an empty dictionary response")
	}
	if result.Id <= 0 {
		return nil, status.Error(codes.Internal, "system RPC returned an invalid dictionary id")
	}
	return &types.CreateDictionaryItemResp{Id: result.Id}, nil
}
