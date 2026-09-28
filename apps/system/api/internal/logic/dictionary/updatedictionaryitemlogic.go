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

type UpdateDictionaryItemLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateDictionaryItemLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateDictionaryItemLogic {
	return &UpdateDictionaryItemLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateDictionaryItemLogic) UpdateDictionaryItem(req *types.UpdateDictionaryItemReq) (resp *types.EmptyResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid dictionary request")
	}
	result, err := l.svcCtx.SystemRpc.UpdateDictionaryItem(l.ctx, &systemclient.UpdateDictionaryItemRequest{Id: req.Id, Label: req.Label, Sort: req.Sort, Remark: req.Remark})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned an empty dictionary response")
	}
	return &types.EmptyResp{}, nil
}
