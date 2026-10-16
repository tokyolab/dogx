package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListJobsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListJobsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListJobsLogic {
	return &ListJobsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListJobsLogic) ListJobs(in *job.ListJobsRequest) (*job.ListJobsResponse, error) {
	filter, err := normalizeFilter(in)
	if err != nil {
		return nil, err
	}
	items, total, err := l.svcCtx.Jobs.ListJobs(l.ctx, filter)
	if err != nil {
		return nil, serviceError(err)
	}
	result := make([]*job.JobListItem, 0, len(items))
	for _, item := range items {
		result = append(result, listItem(item))
	}
	return &job.ListJobsResponse{Items: result, Total: total}, nil
}
