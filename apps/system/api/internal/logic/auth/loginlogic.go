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

type LoginLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

type LoginMetadata struct {
	IPAddress string
	UserAgent string
}

// Sign in with username and password
func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *LoginLogic) Login(req *types.LoginReq, metadata LoginMetadata) (resp *types.LoginResp, err error) {
	result, err := l.svcCtx.SystemRpc.Login(l.ctx, &systemclient.LoginRequest{
		Username:  req.Username,
		Password:  req.Password,
		IpAddress: metadata.IPAddress,
		UserAgent: metadata.UserAgent,
	})
	if err != nil {
		return nil, err
	}

	return &types.LoginResp{
		AccessToken:  result.AccessToken,
		RefreshToken: result.RefreshToken,
		ExpiresIn:    result.ExpiresIn,
	}, nil
}
