package logic

import (
	"context"
	"math"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListInstancesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListInstancesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListInstancesLogic {
	return &ListInstancesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListInstancesLogic) ListInstances(in *job.ListInstancesRequest) (*job.ListInstancesResponse, error) {
	filter, err := normalizeInstanceFilter(in)
	if err != nil {
		return nil, err
	}
	items, total, err := l.svcCtx.InstanceRepo.List(l.ctx, filter)
	if err != nil {
		return nil, rpcOperationError("list job instances", err)
	}
	result := make([]*job.InstanceListItem, 0, len(items))
	for _, item := range items {
		result = append(result, &job.InstanceListItem{
			Id: item.ID, JobId: item.JobID, JobName: item.JobName, ExecutorGroupId: item.ExecutorGroupID,
			ExecutorGroupName: item.ExecutorGroupName, TriggerType: int32(item.TriggerType), ScheduledAt: executionTime(item.ScheduledAt),
			Status: int32(item.Status), StartedAt: executionTime(item.StartedAt), DeadlineAt: executionTime(item.DeadlineAt),
			FinishedAt: executionTime(item.FinishedAt), EndReason: item.EndReason,
			CreatedAt: item.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: item.UpdatedAt.Format(time.RFC3339Nano),
		})
	}
	return &job.ListInstancesResponse{Items: result, Total: total}, nil
}

func normalizeInstanceFilter(in *job.ListInstancesRequest) (repository.InstanceFilter, error) {
	if in == nil || in.Page < 1 || in.PageSize < 1 || in.PageSize > 200 || in.Page-1 > math.MaxInt64/in.PageSize ||
		(in.Id != nil && *in.Id <= 0) || (in.JobId != nil && *in.JobId <= 0) || (in.ExecutorGroupId != nil && *in.ExecutorGroupId <= 0) ||
		(in.Status != nil && (*in.Status < int32(model.InstanceWaiting) || *in.Status > int32(model.InstanceStopping))) ||
		(in.TriggerType != nil && (*in.TriggerType < int32(model.TriggerScheduled) || *in.TriggerType > int32(model.TriggerManual))) {
		return repository.InstanceFilter{}, invalidInput()
	}
	name, err := normalizeText(in.JobName, false, 128)
	if err != nil {
		return repository.InstanceFilter{}, err
	}
	filter := repository.InstanceFilter{Page: in.Page, PageSize: in.PageSize, ID: in.Id, JobID: in.JobId,
		JobName: name, ExecutorGroupID: in.ExecutorGroupId}
	if in.Status != nil {
		value := model.InstanceStatus(*in.Status)
		filter.Status = &value
	}
	if in.TriggerType != nil {
		value := model.TriggerType(*in.TriggerType)
		filter.TriggerType = &value
	}
	if filter.CreatedFrom, err = instanceFilterTime(in.CreatedFrom); err != nil {
		return repository.InstanceFilter{}, err
	}
	if filter.CreatedTo, err = instanceFilterTime(in.CreatedTo); err != nil {
		return repository.InstanceFilter{}, err
	}
	if filter.CreatedFrom != nil && filter.CreatedTo != nil && !filter.CreatedFrom.Before(*filter.CreatedTo) {
		return repository.InstanceFilter{}, invalidInput()
	}
	return filter, nil
}

func instanceFilterTime(raw string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || value.IsZero() || value.Year() < 1 {
		return nil, invalidInput()
	}
	return &value, nil
}
