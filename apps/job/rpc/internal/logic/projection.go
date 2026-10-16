package logic

import (
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
)

func nextTime(status model.Status, value *time.Time) *string {
	if status != model.StatusEnabled || value == nil {
		return nil
	}
	text := value.Format(time.RFC3339Nano)
	return &text
}
func listItem(value repository.JobListItem) *job.JobListItem {
	return &job.JobListItem{Id: value.ID, Name: value.Name, ExecutorGroupId: value.ExecutorGroupID, ExecutorGroupName: value.ExecutorGroupName, ProcessorName: value.ProcessorName, ExecutionMode: int32(value.ExecutionMode), CronExpr: value.CronExpr, Status: int32(value.Status), NextTriggerAt: nextTime(value.Status, value.NextTriggerAt), CreatedAt: value.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: value.UpdatedAt.Format(time.RFC3339Nano)}
}
func jobInfo(value *repository.JobDetail) *job.JobInfo {
	return &job.JobInfo{Id: value.ID, Name: value.Name, ExecutorGroupId: value.ExecutorGroupID, ExecutorGroupName: value.ExecutorGroupName, ProcessorName: value.ProcessorName, ExecutionMode: int32(value.ExecutionMode), CronExpr: value.CronExpr, Status: int32(value.Status), NextTriggerAt: nextTime(value.Status, value.NextTriggerAt), CreatedAt: value.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: value.UpdatedAt.Format(time.RFC3339Nano), Remark: value.Remark, PostEnabled: value.PostEnabled, Params: value.Params, MisfirePolicy: int32(value.MisfirePolicy), BlockPolicy: int32(value.BlockPolicy), MaxRetries: value.MaxRetries, RetryIntervalSeconds: value.RetryIntervalSeconds, TimeoutSeconds: value.TimeoutSeconds}
}
