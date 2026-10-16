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

type CreateJobLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateJobLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateJobLogic {
	return &CreateJobLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateJobLogic) CreateJob(req *types.CreateJobReq) (resp *types.CreateJobResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.CreateJob(l.ctx, &jobclient.CreateJobRequest{Config: &jobclient.JobInput{Name: req.Name, Remark: req.Remark, ExecutorGroupId: req.ExecutorGroupId, ProcessorName: req.ProcessorName, ExecutionMode: req.ExecutionMode, PostEnabled: req.PostEnabled, Params: req.Params, CronExpr: req.CronExpr, MisfirePolicy: req.MisfirePolicy, BlockPolicy: req.BlockPolicy, MaxRetries: req.MaxRetries, RetryIntervalSeconds: req.RetryIntervalSeconds, TimeoutSeconds: req.TimeoutSeconds}, Status: req.Status})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Id <= 0 {
		return nil, invalidResponse()
	}
	return &types.CreateJobResp{Id: result.Id}, nil
}
