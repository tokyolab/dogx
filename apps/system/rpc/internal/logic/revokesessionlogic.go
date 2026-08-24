package logic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RevokeSessionLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRevokeSessionLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RevokeSessionLogic {
	return &RevokeSessionLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RevokeSessionLogic) RevokeSession(in *system.RevokeSessionRequest) (*system.EmptyResponse, error) {
	if in == nil || in.UserId <= 0 || strings.TrimSpace(in.SessionId) == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid session revocation request")
	}

	if err := l.svcCtx.Sessions.Revoke(l.ctx, in.UserId, in.SessionId); err != nil {
		if errors.Is(err, authn.ErrSessionUserMismatch) {
			return nil, status.Error(codes.PermissionDenied, "session does not belong to user")
		}
		return nil, fmt.Errorf("revoke session: %w", err)
	}

	return &system.EmptyResponse{}, nil
}
