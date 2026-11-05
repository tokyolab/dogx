package logic

import (
	"context"
	"math"

	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListInstanceTasksLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListInstanceTasksLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListInstanceTasksLogic {
	return &ListInstanceTasksLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListInstanceTasksLogic) ListInstanceTasks(in *job.ListInstanceTasksRequest) (*job.ListInstanceTasksResponse, error) {
	if in == nil || in.InstanceId <= 0 || in.Page < 1 || in.PageSize < 1 || in.PageSize > 200 || in.Page-1 > math.MaxInt64/in.PageSize {
		return nil, invalidInput()
	}
	exists, err := l.svcCtx.InstanceRepo.Exists(l.ctx, in.InstanceId)
	if err != nil {
		return nil, instanceQueryError("find job instance for task list", err)
	}
	if !exists {
		return nil, instanceNotFound()
	}
	items, total, err := l.svcCtx.TaskRepo.ListByInstance(l.ctx, in.InstanceId, in.Page, in.PageSize)
	if err != nil {
		return nil, rpcOperationError("list instance tasks", err)
	}
	result := make([]*job.TaskListItem, 0, len(items))
	for _, item := range items {
		result = append(result, taskListItem(item))
	}
	return &job.ListInstanceTasksResponse{Items: result, Total: total}, nil
}
