// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package api

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ListAPIsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// List API authorization resources
func NewListAPIsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListAPIsLogic {
	return &ListAPIsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListAPIsLogic) ListAPIs(req *types.APIListReq) (resp *types.APIListResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid API list request")
	}
	result, err := l.svcCtx.SystemRpc.ListAPIs(l.ctx, &systemclient.ListAPIsRequest{
		Keyword:     req.Keyword,
		ServiceName: req.ServiceName,
		ApiGroup:    req.Group,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("system RPC returned an empty API list")
	}

	items := make([]types.APIItem, 0, len(result.Items))
	for _, item := range result.Items {
		if item == nil {
			return nil, errors.New("system RPC returned an invalid API item")
		}
		items = append(items, types.APIItem{
			Id:          item.Id,
			ServiceName: item.ServiceName,
			Group:       item.ApiGroup,
			Name:        item.Name,
			Path:        item.Path,
			Method:      item.Method,
			IsRequired:  item.IsRequired,
			Status:      item.Status,
			Remark:      item.Remark,
		})
	}
	return &types.APIListResp{Items: items}, nil
}
