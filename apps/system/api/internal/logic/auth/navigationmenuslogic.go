// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package auth

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type NavigationMenusLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Return enabled navigation for signed-in users
func NewNavigationMenusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *NavigationMenusLogic {
	return &NavigationMenusLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *NavigationMenusLogic) NavigationMenus() (resp *types.NavigationMenusResp, err error) {
	identity, err := authenticatedIdentity(l.ctx)
	if err != nil {
		return nil, err
	}
	result, err := l.svcCtx.SystemRpc.ListNavigationMenus(l.ctx, &systemclient.ListNavigationMenusRequest{RoleIds: identity.RoleIDs, IsSuperAdmin: identity.IsSuperAdmin})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("invalid navigation response")
	}
	items := make([]types.NavigationMenu, 0, len(result.Items))
	for _, item := range result.Items {
		if item == nil {
			return nil, errors.New("invalid navigation item")
		}
		items = append(items, types.NavigationMenu{
			Id: item.Id, ParentId: item.ParentId, Type: item.Type,
			Name: item.Name, RouteName: item.RouteName, Path: item.Path,
			Component: item.Component, Icon: item.Icon, Sort: item.Sort,
			Visible: item.Visible, KeepAlive: item.KeepAlive, External: item.External,
		})
	}
	return &types.NavigationMenusResp{Items: items, Permissions: append([]string{}, result.Permissions...)}, nil
}
