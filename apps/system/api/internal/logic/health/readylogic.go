package health

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/response"

	"github.com/zeromicro/go-zero/core/logx"
)

type ReadyLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewReadyLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReadyLogic {
	return &ReadyLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ReadyLogic) Ready() (*types.ReadyResp, error) {
	checkCtx, cancel := context.WithTimeout(l.ctx, l.svcCtx.Config.App.ReadinessTimeout)
	defer cancel()
	if l.svcCtx.AuthorizationReadiness == nil {
		return nil, response.ServiceUnavailable(errors.New("API authorization is not ready"))
	}
	if err := l.svcCtx.AuthorizationReadiness.Check(checkCtx); err != nil {
		return nil, response.ServiceUnavailable(err)
	}
	if l.svcCtx.Redis == nil || !l.svcCtx.Redis.PingCtx(checkCtx) {
		return nil, response.ServiceUnavailable(errors.New("API Redis is not ready"))
	}

	rpcResponse, err := l.svcCtx.SystemRpc.CheckReady(checkCtx, &systemclient.ReadyRequest{})
	if err != nil {
		return nil, response.ServiceUnavailable(err)
	}

	return &types.ReadyResp{Status: rpcResponse.Status}, nil
}
