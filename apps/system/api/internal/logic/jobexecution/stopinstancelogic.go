// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package jobexecution

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type StopInstanceLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewStopInstanceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StopInstanceLogic {
	return &StopInstanceLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *StopInstanceLogic) StopInstance(req *types.IDReq) (resp *types.JobInstanceStopResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.StopInstance(l.ctx, &jobclient.StopInstanceRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidResponse()
	}
	return &types.JobInstanceStopResp{
		InstanceId: result.InstanceId, Status: result.Status, EndReason: result.EndReason,
	}, nil
}
