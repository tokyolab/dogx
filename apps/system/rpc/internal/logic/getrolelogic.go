package logic

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
	systemsubcode "github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetRoleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetRoleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetRoleLogic {
	return &GetRoleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetRoleLogic) GetRole(in *system.GetRoleRequest) (*system.GetRoleResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid role request")
	}
	if l.svcCtx.RoleRepo == nil {
		return nil, errors.New("role repository is unavailable")
	}

	role, err := l.svcCtx.RoleRepo.FindByID(l.ctx, in.Id)
	if errors.Is(err, repository.ErrRoleNotFound) {
		return nil, bizerror.New(systemsubcode.RoleNotFound, "角色不存在")
	}
	if err != nil {
		return nil, fmt.Errorf("find role: %w", err)
	}
	return &system.GetRoleResponse{Role: toRoleInfo(*role)}, nil
}
