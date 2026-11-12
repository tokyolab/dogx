package jobexecution

import (
	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
)

func taskProjection(item *jobclient.TaskListItem) types.JobTaskListItem {
	return types.JobTaskListItem{
		Id: item.Id, InstanceId: item.InstanceId, TaskType: item.TaskType, Status: item.Status,
		AttemptNo: item.AttemptNo, UsedAttempts: item.UsedAttempts, AttemptRegisteredAt: item.AttemptRegisteredAt,
		NextDispatchAt: item.NextDispatchAt, ExecutorInstanceId: item.ExecutorInstanceId, ExecutorAddress: item.ExecutorAddress,
		ResultSummary: item.ResultSummary, EndReason: item.EndReason, StartedAt: item.StartedAt,
		FinishedAt: item.FinishedAt, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
	}
}
