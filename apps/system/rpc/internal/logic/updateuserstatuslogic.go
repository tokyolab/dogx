package logic

import (
	"context"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateUserStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateUserStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateUserStatusLogic {
	return &UpdateUserStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateUserStatusLogic) UpdateUserStatus(in *system.UpdateUserStatusRequest) (*system.EmptyResponse, error) {
	if in == nil || !validRecordStatus(in.Status) {
		return nil, status.Error(codes.InvalidArgument, "invalid user status")
	}
	if _, err := managedUser(l.ctx, l.svcCtx, in.OperatorId, in.Id, in.Status == 0); err != nil {
		return nil, err
	}
	if in.Status == 0 {
		// Keep Redis outside the PostgreSQL transaction. A revocation failure must
		// not report a successful account deactivation.
		if err := l.svcCtx.Sessions.RevokeAll(l.ctx, in.Id); err != nil {
			return nil, fmt.Errorf("revoke sessions before disabling user: %w", err)
		}
	}
	if err := l.svcCtx.UserRepo.UpdateStatus(l.ctx, in.Id, model.RecordStatus(in.Status)); err != nil {
		return nil, userManagementError(err)
	}
	return &system.EmptyResponse{}, nil
}
