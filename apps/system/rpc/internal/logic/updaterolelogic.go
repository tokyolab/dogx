package logic

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UpdateRoleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateRoleLogic {
	return &UpdateRoleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateRoleLogic) UpdateRole(in *system.UpdateRoleRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || in.Sort < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid update role request")
	}
	normalized, err := normalizeRoleInput(in.Code, in.Name, in.Description)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid update role request")
	}
	if l.svcCtx.RoleWriter == nil {
		return nil, errors.New("role writer is unavailable")
	}

	err = l.svcCtx.RoleWriter.Update(l.ctx, in.Id, repository.RoleUpdate{
		Code:        normalized.code,
		Name:        normalized.name,
		Description: normalized.description,
		Sort:        in.Sort,
	})
	switch {
	case errors.Is(err, repository.ErrRoleNotFound):
		return nil, bizerror.New("角色不存在")
	case errors.Is(err, repository.ErrRoleCodeExists):
		return nil, bizerror.New("角色编码已存在")
	case errors.Is(err, repository.ErrReservedRoleCode):
		return nil, bizerror.New("角色编码为系统保留，不能使用")
	case errors.Is(err, repository.ErrSystemRoleProtected):
		return nil, bizerror.New("系统内置角色编码不可修改")
	case err != nil:
		return nil, fmt.Errorf("update role: %w", err)
	}

	return &system.EmptyResponse{}, nil
}
