package logic

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func instanceQueryError(operation string, err error) error {
	if errors.Is(err, repository.ErrInstanceNotFound) {
		return instanceNotFound()
	}
	return rpcOperationError(operation, err)
}

func instanceNotFound() error {
	return bizerror.New(subcode.InstanceNotFound, "任务实例不存在")
}

func missingInstanceRecord() error {
	return status.Error(codes.Internal, "instance repository returned a nil record without an error")
}

func executionTime(value *time.Time) *string {
	if value == nil {
		return nil
	}
	text := value.Format(time.RFC3339Nano)
	return &text
}

func executionUUID(value *uuid.UUID) *string {
	if value == nil {
		return nil
	}
	text := value.String()
	return &text
}

func instanceInfo(value *model.JobInstance) *job.InstanceInfo {
	config := value.ExecutionConfig
	return &job.InstanceInfo{
		Id: value.ID, JobId: value.JobID, JobName: value.JobName, ExecutorGroupId: value.ExecutorGroupID,
		TriggerType: int32(value.TriggerType), ScheduledAt: executionTime(value.ScheduledAt), RequestId: executionUUID(value.RequestID),
		ParamsOverridden: value.ParamsOverridden, TriggeredBy: value.TriggeredBy,
		ExecutionConfig: &job.ExecutionConfigInfo{
			SchemaVersion: config.SchemaVersion, ProcessorName: config.ProcessorName, ExecutionMode: int32(config.ExecutionMode),
			PostEnabled: config.PostEnabled, Params: config.Params, MaxRetries: config.MaxRetries,
			RetryIntervalSeconds: config.RetryIntervalSeconds, TimeoutSeconds: config.TimeoutSeconds,
		},
		Status: int32(value.Status), StartedAt: executionTime(value.StartedAt), DeadlineAt: executionTime(value.DeadlineAt),
		FinishedAt: executionTime(value.FinishedAt), EndReason: value.EndReason, ResultSummary: value.ResultSummary,
		CreatedAt: value.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: value.UpdatedAt.Format(time.RFC3339Nano),
	}
}

func taskListItem(value repository.TaskSummary) *job.TaskListItem {
	return &job.TaskListItem{
		Id: value.ID, InstanceId: value.InstanceID, TaskType: int32(value.TaskType), Status: int32(value.Status),
		AttemptNo: value.AttemptNo, UsedAttempts: value.UsedAttempts, AttemptRegisteredAt: executionTime(value.AttemptRegisteredAt),
		NextDispatchAt: executionTime(value.NextDispatchAt), ExecutorInstanceId: executionUUID(value.ExecutorInstanceID),
		ExecutorAddress: value.ExecutorAddress, ResultSummary: value.ResultSummary, EndReason: value.EndReason,
		StartedAt: executionTime(value.StartedAt), FinishedAt: executionTime(value.FinishedAt),
		CreatedAt: value.CreatedAt.Format(time.RFC3339Nano), UpdatedAt: value.UpdatedAt.Format(time.RFC3339Nano),
	}
}
