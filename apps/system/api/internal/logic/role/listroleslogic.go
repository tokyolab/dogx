// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package role

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

type ListRolesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// List roles
func NewListRolesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListRolesLogic {
	return &ListRolesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListRolesLogic) ListRoles(req *types.RoleListReq) (resp *types.RoleListResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid role list request")
	}
	result, err := l.svcCtx.SystemRpc.ListRoles(l.ctx, &systemclient.ListRolesRequest{
		Page:     req.Page,
		PageSize: req.PageSize,
		Keyword:  req.Keyword,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("system RPC returned an empty role list")
	}

	items := make([]types.RoleItem, 0, len(result.Items))
	for _, item := range result.Items {
		mapped := toRoleItem(item)
		if mapped == nil {
			return nil, errors.New("system RPC returned an invalid role item")
		}
		items = append(items, *mapped)
	}
	return &types.RoleListResp{Items: items, Total: result.Total}, nil
}
