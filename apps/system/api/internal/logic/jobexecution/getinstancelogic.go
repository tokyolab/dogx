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

type GetInstanceLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetInstanceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetInstanceLogic {
	return &GetInstanceLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetInstanceLogic) GetInstance(req *types.IDReq) (resp *types.JobInstanceDetail, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.GetInstance(l.ctx, &jobclient.GetInstanceRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Instance == nil || result.Instance.ExecutionConfig == nil {
		return nil, invalidResponse()
	}
	item, config := result.Instance, result.Instance.ExecutionConfig
	var stats *types.JobShardStatistics
	if value := item.ShardStats; value != nil {
		stats = &types.JobShardStatistics{Total: value.Total, Succeeded: value.Succeeded,
			Failed: value.Failed, Canceled: value.Canceled, Unfinished: value.Unfinished}
	}
	return &types.JobInstanceDetail{
		Id: item.Id, JobId: item.JobId, JobName: item.JobName, ExecutorGroupId: item.ExecutorGroupId,
		TriggerType: item.TriggerType, ScheduledAt: item.ScheduledAt, RequestId: item.RequestId,
		ParamsOverridden: item.ParamsOverridden, TriggeredBy: item.TriggeredBy,
		ExecutionConfig: types.JobExecutionConfig{
			SchemaVersion: config.SchemaVersion, ProcessorName: config.ProcessorName, ExecutionMode: config.ExecutionMode,
			PostEnabled: config.PostEnabled, Params: config.Params, MaxRetries: config.MaxRetries,
			RetryIntervalSeconds: config.RetryIntervalSeconds, TimeoutSeconds: config.TimeoutSeconds,
		},
		Status: item.Status, StartedAt: item.StartedAt, DeadlineAt: item.DeadlineAt, FinishedAt: item.FinishedAt,
		EndReason: item.EndReason, ResultSummary: item.ResultSummary, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt, ShardStats: stats,
	}, nil
}
