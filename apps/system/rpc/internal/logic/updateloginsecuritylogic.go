package logic

import (
	"context"
	"github.com/tokyolab/dogx/apps/system/internal/loginprotection"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateLoginSecurityLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateLoginSecurityLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateLoginSecurityLogic {
	return &UpdateLoginSecurityLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateLoginSecurityLogic) UpdateLoginSecurity(in *system.LoginSecurityConfig) (*system.EmptyResponse, error) {
	if err := loginprotection.Validate(in); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	err := l.svcCtx.SecurityRepo.UpdateLogin(l.ctx, &model.SecurityConfig{
		LoginRateLimitEnabled: in.RateLimitEnabled, LoginRateLimitWindowSeconds: in.RateLimitWindowSeconds,
		LoginRateLimitMaxRequests: in.RateLimitMaxRequests, LoginFailureLockEnabled: in.FailureLockEnabled,
		LoginFailureWindowSeconds: in.FailureWindowSeconds, LoginFailureThreshold: in.FailureThreshold,
		LoginLockDurationSeconds: in.LockDurationSeconds,
	})
	if err != nil {
		return nil, err
	}
	// The database is committed. Notify peers even if local reloading fails.
	reloadErr := l.svcCtx.Security.Reload(l.ctx)
	notifyErr := l.svcCtx.Security.Notify(l.ctx)
	if reloadErr != nil || notifyErr != nil {
		l.Errorf("saved login security but synchronization failed: reload=%v notify=%v", reloadErr, notifyErr)
		return nil, bizerror.New(subcode.SecuritySyncFailed, "配置已保存，但同步失败，请稍后重试")
	}

	return &system.EmptyResponse{}, nil
}
