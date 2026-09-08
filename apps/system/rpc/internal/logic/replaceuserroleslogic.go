package logic

import (
	"context"
	"fmt"
	"slices"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ReplaceUserRolesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewReplaceUserRolesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReplaceUserRolesLogic {
	return &ReplaceUserRolesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ReplaceUserRolesLogic) ReplaceUserRoles(in *system.ReplaceUserRolesRequest) (*system.EmptyResponse, error) {
	if in == nil || !validUserRoleIDs(in.RoleIds) {
		return nil, status.Error(codes.InvalidArgument, "invalid user roles")
	}
	record, err := managedUser(l.ctx, l.svcCtx, in.OperatorId, in.Id, false)
	if err != nil {
		return nil, err
	}
	// Self-managed profile and credentials do not make the super administrator's
	// roles editable. Reject before any session revocation, including no-op saves.
	if userHasSuperRole(record) {
		return nil, userManagementError(repository.ErrSuperAdminProtected)
	}
	if err := l.svcCtx.UserRepo.ValidateRoles(l.ctx, in.RoleIds, record.Roles); err != nil {
		return nil, userManagementError(err)
	}
	// Check only after validation so an unchanged request cannot bypass grant
	// restrictions. Reuse the loaded roles instead of revoking sessions first.
	if sameUserRoleIDs(record.Roles, in.RoleIds) {
		return &system.EmptyResponse{}, nil
	}
	// Revoke first, as in password changes. A later database failure can log the
	// user out, but must not leave pre-change role credentials intentionally valid.
	if err := l.svcCtx.Sessions.RevokeAll(l.ctx, in.Id); err != nil {
		return nil, fmt.Errorf("revoke sessions before replacing user roles: %w", err)
	}
	if err := l.svcCtx.UserRepo.ReplaceRoles(l.ctx, in.Id, in.RoleIds); err != nil {
		return nil, userManagementError(err)
	}
	return &system.EmptyResponse{}, nil
}

func sameUserRoleIDs(current []model.Role, targetIDs []int64) bool {
	currentIDs := make([]int64, 0, len(current))
	for _, role := range current {
		currentIDs = append(currentIDs, role.ID)
	}
	targetIDs = slices.Clone(targetIDs)
	slices.Sort(currentIDs)
	slices.Sort(targetIDs)
	return slices.Equal(slices.Compact(currentIDs), slices.Compact(targetIDs))
}
