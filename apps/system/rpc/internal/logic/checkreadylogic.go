package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CheckReadyLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCheckReadyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CheckReadyLogic {
	return &CheckReadyLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CheckReadyLogic) CheckReady(in *system.ReadyRequest) (*system.ReadyResponse, error) {
	checkCtx, cancel := context.WithTimeout(l.ctx, l.svcCtx.Config.App.ReadinessTimeout)
	defer cancel()

	if err := l.svcCtx.Readiness.Check(checkCtx); err != nil {
		l.Errorf("readiness check failed: %v", err)
		return nil, status.Error(codes.Unavailable, "service dependencies are not ready")
	}

	return &system.ReadyResponse{Status: "ready"}, nil
}
