package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ReplaceRoleMenusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewReplaceRoleMenusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReplaceRoleMenusLogic {
	return &ReplaceRoleMenusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ReplaceRoleMenusLogic) ReplaceRoleMenus(in *system.ReplaceRoleMenusRequest) (*system.EmptyResponse, error) {
	if in == nil || in.RoleId <= 0 || len(in.MenuIds) > 10000 {
		return nil, invalidMenuRequest()
	}
	for _, id := range in.MenuIds {
		if id <= 0 {
			return nil, invalidMenuRequest()
		}
	}
	if l.svcCtx.RoleMenuRepo == nil {
		return nil, errors.New("role menu repository is unavailable")
	}
	if err := l.svcCtx.RoleMenuRepo.Replace(l.ctx, in.RoleId, in.MenuIds); err != nil {
		return nil, roleMenuBusinessError(err)
	}
	return &system.EmptyResponse{}, nil
}
