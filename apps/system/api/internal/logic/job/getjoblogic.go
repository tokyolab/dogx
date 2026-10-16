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

type GetJobLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetJobLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetJobLogic {
	return &GetJobLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetJobLogic) GetJob(req *types.IDReq) (resp *types.JobDetail, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.GetJob(l.ctx, &jobclient.GetJobRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Job == nil {
		return nil, invalidResponse()
	}
	item := result.Job
	return &types.JobDetail{Id: item.Id, Name: item.Name, ExecutorGroupId: item.ExecutorGroupId, ExecutorGroupName: item.ExecutorGroupName, ProcessorName: item.ProcessorName, ExecutionMode: item.ExecutionMode, CronExpr: item.CronExpr, Status: item.Status, NextTriggerAt: item.NextTriggerAt, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt, Remark: item.Remark, PostEnabled: item.PostEnabled, Params: item.Params, MisfirePolicy: item.MisfirePolicy, BlockPolicy: item.BlockPolicy, MaxRetries: item.MaxRetries, RetryIntervalSeconds: item.RetryIntervalSeconds, TimeoutSeconds: item.TimeoutSeconds}, nil
}
