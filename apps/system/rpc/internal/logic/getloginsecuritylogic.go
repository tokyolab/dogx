package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetLoginSecurityLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetLoginSecurityLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetLoginSecurityLogic {
	return &GetLoginSecurityLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetLoginSecurityLogic) GetLoginSecurity(in *system.GetLoginSecurityRequest) (*system.LoginSecurityConfig, error) {
	return svc.LoadLoginSecurity(l.ctx, l.svcCtx.SecurityRepo)
}
