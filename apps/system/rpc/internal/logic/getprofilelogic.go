package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetProfileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetProfileLogic {
	return &GetProfileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetProfileLogic) GetProfile(in *system.CurrentUserRequest) (*system.ProfileResponse, error) {
	if in == nil || in.UserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid profile request")
	}
	record, err := l.svcCtx.UserRepo.FindWithRoles(l.ctx, in.UserId)
	if errors.Is(err, repository.ErrUserNotFound) {
		return nil, status.Error(codes.Unauthenticated, "user no longer exists")
	}
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, status.Error(codes.Internal, "user repository returned no user")
	}
	if record.User.Status != model.RecordStatusEnabled {
		return nil, status.Error(codes.Unauthenticated, "user is disabled")
	}
	result := &system.ProfileResponse{Username: record.User.Username, Nickname: record.User.Nickname,
		DepartmentName: record.DepartmentName, Roles: make([]string, 0, len(record.Roles))}
	if record.User.Email != nil {
		result.Email = *record.User.Email
	}
	if record.User.Phone != nil {
		result.Phone = *record.User.Phone
	}
	for _, role := range record.Roles {
		result.Roles = append(result.Roles, role.Name)
	}
	return result, nil
}
