package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListNavigationMenusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListNavigationMenusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListNavigationMenusLogic {
	return &ListNavigationMenusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListNavigationMenusLogic) ListNavigationMenus(in *system.ListNavigationMenusRequest) (*system.ListNavigationMenusResponse, error) {
	if in == nil {
		return nil, invalidMenuRequest()
	}
	if l.svcCtx.MenuRepo == nil {
		return nil, errors.New("menu repository is unavailable")
	}

	for _, id := range in.RoleIds {
		if id <= 0 {
			return nil, invalidMenuRequest()
		}
	}
	menus, err := l.svcCtx.MenuRepo.List(l.ctx)
	if err != nil {
		return nil, menuBusinessError(err)
	}

	granted := make(map[int64]bool)
	if !in.IsSuperAdmin && len(in.RoleIds) > 0 {
		if l.svcCtx.RoleMenuRepo == nil {
			return nil, errors.New("role menu repository is unavailable")
		}
		ids, err := l.svcCtx.RoleMenuRepo.ListMenuIDs(l.ctx, in.RoleIds)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			granted[id] = true
		}
	}
	children := make(map[int64][]model.Menu)
	for _, menu := range menus {
		if menu.AppCode != model.MenuAppAdminWeb || menu.Status != model.RecordStatusEnabled ||
			(menu.Type != model.MenuTypeDirectory && menu.Type != model.MenuTypePage && menu.Type != model.MenuTypeElement) || (!in.IsSuperAdmin && !granted[menu.ID]) {
			continue
		}
		var parentID int64
		if menu.ParentID != nil {
			parentID = *menu.ParentID
		}
		children[parentID] = append(children[parentID], menu)
	}

	// Traverse only from enabled roots: a disabled/missing parent excludes its whole
	// subtree, without changing descendants' stored status. Hidden nodes remain routable.
	items := make([]*system.NavigationMenu, 0, len(menus))
	permissions := make([]string, 0)
	permissionSet := make(map[string]bool)
	queue := []int64{0}
	visited := make(map[int64]bool, len(menus))
	for i := 0; i < len(queue); i++ {
		parentID := queue[i]
		for _, menu := range children[parentID] {
			if menu.ID <= 0 || visited[menu.ID] {
				continue
			}
			visited[menu.ID] = true
			if menu.Type == model.MenuTypeElement {
				if menu.Permission != "" && !permissionSet[menu.Permission] {
					permissions = append(permissions, menu.Permission)
					permissionSet[menu.Permission] = true
				}
				continue
			}
			queue = append(queue, menu.ID)
			items = append(items, &system.NavigationMenu{
				Id: menu.ID, ParentId: parentID, Type: int32(menu.Type),
				Name: menu.Name, RouteName: menu.RouteName, Path: menu.Path,
				Component: menu.Component, Icon: menu.Icon, Sort: menu.Sort,
				Visible: menu.Visible, KeepAlive: menu.KeepAlive, External: menu.External,
			})
		}
	}
	return &system.ListNavigationMenusResponse{Items: items, Permissions: permissions}, nil
}
