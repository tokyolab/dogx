// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package security

import (
	"context"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetLoginSecurityLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetLoginSecurityLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetLoginSecurityLogic {
	return &GetLoginSecurityLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetLoginSecurityLogic) GetLoginSecurity() (resp *types.LoginSecurityConfig, err error) {
	cfg, err := l.svcCtx.SystemRpc.GetLoginSecurity(l.ctx, &system.GetLoginSecurityRequest{})
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, status.Error(codes.Internal, "missing login security config")
	}
	return &types.LoginSecurityConfig{RateLimitEnabled: cfg.RateLimitEnabled, RateLimitWindowSeconds: cfg.RateLimitWindowSeconds, RateLimitMaxRequests: cfg.RateLimitMaxRequests, FailureLockEnabled: cfg.FailureLockEnabled, FailureWindowSeconds: cfg.FailureWindowSeconds, FailureThreshold: cfg.FailureThreshold, LockDurationSeconds: cfg.LockDurationSeconds}, nil
}
