package logic

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxRoleAPIIDs = 10000

type ReplaceRoleAPIsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewReplaceRoleAPIsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReplaceRoleAPIsLogic {
	return &ReplaceRoleAPIsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ReplaceRoleAPIsLogic) ReplaceRoleAPIs(in *system.ReplaceRoleAPIsRequest) (*system.EmptyResponse, error) {
	if in == nil || in.RoleId <= 0 || len(in.ApiIds) > maxRoleAPIIDs {
		return nil, status.Error(codes.InvalidArgument, "invalid role API authorization request")
	}
	if l.svcCtx.RolePolicies == nil {
		return nil, errors.New("role policy service is unavailable")
	}

	result, err := l.svcCtx.RolePolicies.ReplaceRoleAPIs(l.ctx, in.RoleId, in.ApiIds)
	switch {
	case errors.Is(err, authorization.ErrInvalidRoleID):
		return nil, status.Error(codes.InvalidArgument, "invalid role API authorization request")
	case errors.Is(err, authorization.ErrRoleUnavailable):
		return nil, bizerror.New("角色不存在或已停用")
	case errors.Is(err, authorization.ErrAPIUnavailable):
		return nil, bizerror.New("接口资源不存在或已停用")
	case err != nil:
		return nil, fmt.Errorf("replace role API policies: %w", err)
	}
	if result.NotificationError != nil {
		l.Errorf(
			"publish role policy invalidation after database commit: roleId=%d added=%d removed=%d error=%v",
			in.RoleId,
			result.Added,
			result.Removed,
			result.NotificationError,
		)
	}

	return &system.EmptyResponse{}, nil
}
