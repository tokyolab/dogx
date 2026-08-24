// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package auth

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
)

type LogoutLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// Revoke the current session
func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LogoutLogic) Logout() (resp *types.EmptyResp, err error) {
	identity, err := authenticatedIdentity(l.ctx)
	if err != nil {
		return nil, err
	}
	if _, err := l.svcCtx.SystemRpc.RevokeSession(l.ctx, &systemclient.RevokeSessionRequest{
		UserId:    identity.UserID,
		SessionId: identity.SessionID,
	}); err != nil {
		return nil, err
	}

	return &types.EmptyResp{}, nil
}
