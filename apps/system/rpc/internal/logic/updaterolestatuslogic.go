package logic

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	systemsubcode "github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UpdateRoleStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateRoleStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateRoleStatusLogic {
	return &UpdateRoleStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateRoleStatusLogic) UpdateRoleStatus(in *system.UpdateRoleStatusRequest) (*system.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || !validRecordStatus(in.Status) {
		return nil, status.Error(codes.InvalidArgument, "invalid update role status request")
	}
	if l.svcCtx.RolePolicies == nil || l.svcCtx.Sessions == nil {
		return nil, errors.New("role lifecycle dependencies are unavailable")
	}

	err := l.svcCtx.RolePolicies.UpdateRoleStatus(
		l.ctx,
		in.Id,
		model.RecordStatus(in.Status),
		l.svcCtx.Sessions,
	)
	switch {
	case errors.Is(err, repository.ErrRoleNotFound):
		return nil, bizerror.New(systemsubcode.RoleNotFound, "角色不存在")
	case errors.Is(err, repository.ErrSystemRoleProtected):
		return nil, bizerror.New(systemsubcode.RoleSystemCannotDisable, "系统内置角色不能停用")
	case errors.Is(err, authorization.ErrInvalidRoleID):
		return nil, status.Error(codes.InvalidArgument, "invalid update role status request")
	case err != nil:
		return nil, fmt.Errorf("update role status: %w", err)
	}

	return &system.EmptyResponse{}, nil
}
