package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetRoleMenusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetRoleMenusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetRoleMenusLogic {
	return &GetRoleMenusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetRoleMenusLogic) GetRoleMenus(in *system.GetRoleMenusRequest) (*system.GetRoleMenusResponse, error) {
	if in == nil || in.RoleId <= 0 {
		return nil, invalidMenuRequest()
	}
	if l.svcCtx.RoleRepo == nil || l.svcCtx.MenuRepo == nil || l.svcCtx.RoleMenuRepo == nil {
		return nil, errors.New("role menu dependencies are unavailable")
	}
	role, err := l.svcCtx.RoleRepo.FindByID(l.ctx, in.RoleId)
	if err != nil {
		return nil, roleMenuBusinessError(err)
	}
	menus, err := l.svcCtx.MenuRepo.List(l.ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0)
	if role.Code == model.SuperAdminRoleCode {
		for _, menu := range menus {
			ids = append(ids, menu.ID)
		}
	} else {
		ids, err = l.svcCtx.RoleMenuRepo.ListMenuIDs(l.ctx, []int64{in.RoleId})
		if err != nil {
			return nil, err
		}
	}
	items := make([]*system.MenuInfo, 0, len(menus))
	for _, menu := range menus {
		items = append(items, toMenuInfo(menu))
	}
	return &system.GetRoleMenusResponse{Items: items, MenuIds: ids}, nil
}
