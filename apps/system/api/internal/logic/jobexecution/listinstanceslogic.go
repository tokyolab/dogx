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

type ListInstancesLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListInstancesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListInstancesLogic {
	return &ListInstancesLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListInstancesLogic) ListInstances(req *types.JobInstanceListReq) (resp *types.JobInstanceListResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.ListInstances(l.ctx, &jobclient.ListInstancesRequest{
		Page: req.Page, PageSize: req.PageSize, Id: req.Id, JobId: req.JobId,
		JobName: req.JobName, ExecutorGroupId: req.ExecutorGroupId, Status: req.Status,
		TriggerType: req.TriggerType, CreatedFrom: req.CreatedFrom, CreatedTo: req.CreatedTo,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidResponse()
	}
	items := make([]types.JobInstanceListItem, 0, len(result.Items))
	for _, item := range result.Items {
		if item == nil {
			return nil, invalidResponse()
		}
		items = append(items, types.JobInstanceListItem{
			Id: item.Id, JobId: item.JobId, JobName: item.JobName,
			ExecutorGroupId: item.ExecutorGroupId, ExecutorGroupName: item.ExecutorGroupName,
			TriggerType: item.TriggerType, ScheduledAt: item.ScheduledAt, Status: item.Status,
			StartedAt: item.StartedAt, DeadlineAt: item.DeadlineAt, FinishedAt: item.FinishedAt,
			EndReason: item.EndReason,
			CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
		})
	}
	return &types.JobInstanceListResp{Items: items, PageMeta: types.PageMeta{Total: result.Total}}, nil
}
