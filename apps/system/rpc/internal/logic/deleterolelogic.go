package logic

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	systemsubcode "github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DeleteRoleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteRoleLogic {
	return &DeleteRoleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteRoleLogic) DeleteRole(in *system.DeleteRoleRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid delete role request")
	}
	if l.svcCtx.RolePolicies == nil {
		return nil, errors.New("role policy service is unavailable")
	}

	result, err := l.svcCtx.RolePolicies.DeleteRole(l.ctx, in.Id)
	switch {
	case errors.Is(err, repository.ErrRoleNotFound):
		return nil, bizerror.New(systemsubcode.RoleNotFound, "角色不存在")
	case errors.Is(err, repository.ErrRoleInUse):
		return nil, bizerror.New(systemsubcode.RoleInUse, "角色已被用户使用，不能删除")
	case errors.Is(err, repository.ErrSystemRoleProtected):
		return nil, bizerror.New(systemsubcode.RoleSystemCannotDelete, "系统内置角色不能删除")
	case errors.Is(err, authorization.ErrInvalidRoleID):
		return nil, status.Error(codes.InvalidArgument, "invalid delete role request")
	case err != nil:
		return nil, fmt.Errorf("delete role: %w", err)
	}
	if result.NotificationError != nil {
		l.Errorf(
			"publish role policy invalidation after role deletion: roleId=%d removed=%d error=%v",
			in.Id,
			result.RemovedPolicies,
			result.NotificationError,
		)
	}

	return &system.EmptyResponse{}, nil
}
