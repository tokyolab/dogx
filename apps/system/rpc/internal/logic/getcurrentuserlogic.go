package logic

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetCurrentUserLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetCurrentUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetCurrentUserLogic {
	return &GetCurrentUserLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetCurrentUserLogic) GetCurrentUser(in *system.CurrentUserRequest) (*system.CurrentUserResponse, error) {
	if in == nil || in.UserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid current user request")
	}

	user, err := l.svcCtx.UserRepo.FindByID(l.ctx, in.UserId)
	if errors.Is(err, repository.ErrUserNotFound) {
		return nil, status.Error(codes.Unauthenticated, "user no longer exists")
	}
	if err != nil {
		return nil, fmt.Errorf("find current user: %w", err)
	}
	if user.Status != model.RecordStatusEnabled {
		if err := l.svcCtx.Sessions.RevokeAll(l.ctx, user.ID); err != nil {
			return nil, fmt.Errorf("revoke disabled user sessions: %w", err)
		}
		return nil, status.Error(codes.Unauthenticated, "user is disabled")
	}

	return &system.CurrentUserResponse{
		Id:       user.ID,
		Username: user.Username,
		Nickname: user.Nickname,
	}, nil
}
