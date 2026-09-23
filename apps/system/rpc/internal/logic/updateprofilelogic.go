package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UpdateProfileLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateProfileLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateProfileLogic {
	return &UpdateProfileLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateProfileLogic) UpdateProfile(in *system.UpdateProfileRequest) (*system.EmptyResponse, error) {
	if in == nil || in.UserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid profile request")
	}
	profile, err := normalizeUserProfile(in.Nickname, in.Email, in.Phone, "")
	if err != nil {
		return nil, err
	}
	err = l.svcCtx.UserRepo.UpdateContacts(l.ctx, in.UserId, repository.UserContactUpdate{
		Nickname: profile.Nickname, Email: profile.Email, Phone: profile.Phone,
	})
	if errors.Is(err, repository.ErrUserNotFound) {
		return nil, status.Error(codes.Unauthenticated, "user no longer exists or is disabled")
	}
	if err != nil {
		return nil, userManagementError(err)
	}
	return &system.EmptyResponse{}, nil
}
