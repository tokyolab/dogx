package health

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/common/httperror"

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

	response, err := l.svcCtx.SystemRpc.CheckReady(checkCtx, &systemclient.ReadyRequest{})
	if err != nil {
		return nil, httperror.ServiceUnavailable(err)
	}

	return &types.ReadyResp{Status: response.Status}, nil
}
