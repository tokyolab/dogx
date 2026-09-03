package logic

import (
	"context"
	"errors"
	"fmt"

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

type CreateRoleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateRoleLogic {
	return &CreateRoleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateRoleLogic) CreateRole(in *system.CreateRoleRequest) (*system.CreateRoleResponse, error) {
	if in == nil || in.Sort < 0 || !validRecordStatus(in.Status) {
		return nil, status.Error(codes.InvalidArgument, "invalid create role request")
	}
	normalized, err := normalizeRoleInput(in.Code, in.Name, in.Description)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid create role request")
	}
	if l.svcCtx.RoleRepo == nil {
		return nil, errors.New("role repository is unavailable")
	}

	role := &model.Role{
		Code:        normalized.code,
		Name:        normalized.name,
		Description: normalized.description,
		Sort:        in.Sort,
		Status:      model.RecordStatus(in.Status),
	}
	if err := l.svcCtx.RoleRepo.Create(l.ctx, role); errors.Is(err, repository.ErrRoleCodeExists) {
		return nil, bizerror.New(systemsubcode.RoleCodeExists, "角色编码已存在")
	} else if errors.Is(err, repository.ErrReservedRoleCode) {
		return nil, bizerror.New(systemsubcode.RoleCodeReserved, "角色编码为系统保留，不能使用")
	} else if err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}

	return &system.CreateRoleResponse{Id: role.ID}, nil
}
