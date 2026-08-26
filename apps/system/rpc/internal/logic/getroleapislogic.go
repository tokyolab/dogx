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

type GetRoleAPIsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetRoleAPIsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetRoleAPIsLogic {
	return &GetRoleAPIsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetRoleAPIsLogic) GetRoleAPIs(in *system.GetRoleAPIsRequest) (*system.GetRoleAPIsResponse, error) {
	if in == nil || in.RoleId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid role API request")
	}
	if l.svcCtx.RoleRepo == nil {
		return nil, errors.New("role repository is unavailable")
	}
	if l.svcCtx.RolePolicies == nil {
		return nil, errors.New("role policy service is unavailable")
	}

	if _, err := l.svcCtx.RoleRepo.FindByID(l.ctx, in.RoleId); errors.Is(err, repository.ErrRoleNotFound) {
		return nil, bizerror.New("角色不存在")
	} else if err != nil {
		return nil, fmt.Errorf("find role for API authorization: %w", err)
	}
	apiIDs, err := l.svcCtx.RolePolicies.ListRoleAPIIDs(l.ctx, in.RoleId)
	if err != nil {
		return nil, fmt.Errorf("list role API authorizations: %w", err)
	}
	return &system.GetRoleAPIsResponse{ApiIds: apiIDs}, nil
}
