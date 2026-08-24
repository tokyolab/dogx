package logic

import (
	"context"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RevokeUserSessionsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRevokeUserSessionsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RevokeUserSessionsLogic {
	return &RevokeUserSessionsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RevokeUserSessionsLogic) RevokeUserSessions(in *system.RevokeUserSessionsRequest) (*system.EmptyResponse, error) {
	if in == nil || in.UserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid user session revocation request")
	}

	if err := l.svcCtx.Sessions.RevokeAll(l.ctx, in.UserId); err != nil {
		return nil, fmt.Errorf("revoke user sessions: %w", err)
	}

	return &system.EmptyResponse{}, nil
}
