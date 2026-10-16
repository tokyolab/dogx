// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package job

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateJobLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateJobLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateJobLogic {
	return &UpdateJobLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateJobLogic) UpdateJob(req *types.UpdateJobReq) (resp *types.EmptyResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.UpdateJob(l.ctx, &jobclient.UpdateJobRequest{Id: req.Id, Config: &jobclient.JobInput{Name: req.Name, Remark: req.Remark, ExecutorGroupId: req.ExecutorGroupId, ProcessorName: req.ProcessorName, ExecutionMode: &req.ExecutionMode, PostEnabled: req.PostEnabled, Params: req.Params, CronExpr: req.CronExpr, MisfirePolicy: req.MisfirePolicy, BlockPolicy: req.BlockPolicy, MaxRetries: req.MaxRetries, RetryIntervalSeconds: &req.RetryIntervalSeconds, TimeoutSeconds: req.TimeoutSeconds}})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidResponse()
	}
	return &types.EmptyResp{}, nil
}
