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

type ListJobsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListJobsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListJobsLogic {
	return &ListJobsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListJobsLogic) ListJobs(req *types.JobListReq) (resp *types.JobListResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.ListJobs(l.ctx, &jobclient.ListJobsRequest{Page: req.Page, PageSize: req.PageSize, Id: req.Id, Name: req.Name, ExecutorGroupId: req.ExecutorGroupId, Status: req.Status})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidResponse()
	}
	items := make([]types.JobListItem, 0, len(result.Items))
	for _, item := range result.Items {
		if item == nil {
			return nil, invalidResponse()
		}
		items = append(items, types.JobListItem{Id: item.Id, Name: item.Name, ExecutorGroupId: item.ExecutorGroupId, ExecutorGroupName: item.ExecutorGroupName, ProcessorName: item.ProcessorName, ExecutionMode: item.ExecutionMode, CronExpr: item.CronExpr, Status: item.Status, NextTriggerAt: item.NextTriggerAt, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt})
	}
	return &types.JobListResp{Items: items, Total: result.Total}, nil
}
