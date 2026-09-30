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

type UpdateLoginSecurityLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateLoginSecurityLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateLoginSecurityLogic {
	return &UpdateLoginSecurityLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateLoginSecurityLogic) UpdateLoginSecurity(req *types.LoginSecurityConfig) (resp *types.EmptyResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing login security config")
	}
	_, err = l.svcCtx.SystemRpc.UpdateLoginSecurity(l.ctx, &system.LoginSecurityConfig{RateLimitEnabled: req.RateLimitEnabled, RateLimitWindowSeconds: req.RateLimitWindowSeconds, RateLimitMaxRequests: req.RateLimitMaxRequests, FailureLockEnabled: req.FailureLockEnabled, FailureWindowSeconds: req.FailureWindowSeconds, FailureThreshold: req.FailureThreshold, LockDurationSeconds: req.LockDurationSeconds})
	if err != nil {
		return nil, err
	}
	return &types.EmptyResp{}, nil
}
