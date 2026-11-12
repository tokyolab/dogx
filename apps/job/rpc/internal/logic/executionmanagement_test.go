package logic

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	jobsubcode "github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestListInstancesRejectsInvalidFiltersBeforeQuery(t *testing.T) {
	zeroID, zeroState, largeState := int64(0), int32(0), int32(65537)
	invalidState, invalidTrigger := int32(7), int32(4)
	for _, in := range []*job.ListInstancesRequest{
		nil, {}, {Page: 1, PageSize: 201}, {Page: math.MaxInt64, PageSize: 2},
		{Page: 1, PageSize: 20, Id: &zeroID}, {Page: 1, PageSize: 20, JobId: &zeroID},
		{Page: 1, PageSize: 20, ExecutorGroupId: &zeroID},
		{Page: 1, PageSize: 20, Status: &zeroState}, {Page: 1, PageSize: 20, Status: &invalidState},
		{Page: 1, PageSize: 20, Status: &largeState}, {Page: 1, PageSize: 20, TriggerType: &zeroState},
		{Page: 1, PageSize: 20, TriggerType: &invalidTrigger}, {Page: 1, PageSize: 20, TriggerType: &largeState},
		{Page: 1, PageSize: 20, JobName: strings.Repeat("x", 129)},
		{Page: 1, PageSize: 20, JobName: "invalid\x00name"},
		{Page: 1, PageSize: 20, CreatedFrom: "2026-11-12"},
		{Page: 1, PageSize: 20, CreatedTo: "2026-11-12T10:00:00"},
		{Page: 1, PageSize: 20, CreatedFrom: "0000-11-12T10:00:00Z"},
		{Page: 1, PageSize: 20, CreatedFrom: "2026-11-12T10:00:00Z", CreatedTo: "2026-11-12T11:00:00+01:00"},
		{Page: 1, PageSize: 20, CreatedFrom: "2026-11-12T10:00:01Z", CreatedTo: "2026-11-12T10:00:00Z"},
	} {
		response, err := NewListInstancesLogic(context.Background(), &svc.ServiceContext{}).ListInstances(in)
		if response != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid history filter classification: input=%+v err=%v", in, err)
		}
	}
}

func TestListInstancesProjectsHistoricalFieldsAndAllTriggerTypes(t *testing.T) {
	at := time.Date(2026, 11, 12, 18, 30, 0, 123456000, time.FixedZone("fixture", 8*3600))
	group, reason := "deleted group", ""
	id, jobID, groupID, state := int64(11), int64(12), int64(13), int32(model.InstanceStopping)
	instances := &executionInstanceStub{listTotal: 7, listItems: []repository.InstanceListItem{
		{ID: id, JobID: jobID, JobName: "name at creation", ExecutorGroupID: groupID, ExecutorGroupName: &group,
			TriggerType: model.TriggerMisfire, ScheduledAt: &at, Status: model.InstanceStopping, EndReason: &reason,
			StartedAt: &at, DeadlineAt: &at, CreatedAt: at, UpdatedAt: at},
		{ID: 15, JobID: 16, ExecutorGroupID: 17, TriggerType: model.TriggerManual, Status: model.InstanceWaiting, CreatedAt: at},
	}}
	for trigger := int32(model.TriggerScheduled); trigger <= int32(model.TriggerManual); trigger++ {
		in := &job.ListInstancesRequest{Page: 2, PageSize: 100, Id: &id, JobId: &jobID, JobName: "  saved %_ name  ",
			ExecutorGroupId: &groupID, Status: &state, TriggerType: &trigger, CreatedFrom: at.Format(time.RFC3339Nano),
			CreatedTo: at.Add(time.Hour).Format(time.RFC3339Nano)}
		response, err := NewListInstancesLogic(context.Background(), &svc.ServiceContext{InstanceRepo: instances}).ListInstances(in)
		if err != nil || response.Total != 7 || len(response.Items) != 2 {
			t.Fatalf("history query failed: err=%v", err)
		}
		filter := instances.listFilter
		if filter.ID != &id || filter.JobID != &jobID || filter.ExecutorGroupID != &groupID || filter.JobName != "saved %_ name" ||
			filter.Status == nil || *filter.Status != model.InstanceStopping || filter.TriggerType == nil || int32(*filter.TriggerType) != trigger ||
			filter.Page != 2 || filter.PageSize != 100 || filter.CreatedFrom == nil || !filter.CreatedFrom.Equal(at) ||
			filter.CreatedTo == nil || !filter.CreatedTo.Equal(at.Add(time.Hour)) {
			t.Fatal("history filters lost presence, range semantics or trigger values")
		}
		first, second := response.Items[0], response.Items[1]
		if first.JobName != "name at creation" || first.GetExecutorGroupName() != group || first.EndReason == nil || *first.EndReason != "" ||
			first.GetScheduledAt() != at.Format(time.RFC3339Nano) || first.FinishedAt != nil || first.GetStartedAt() != at.Format(time.RFC3339Nano) ||
			second.ExecutorGroupName != nil || second.ScheduledAt != nil || second.StartedAt != nil || second.EndReason != nil {
			t.Fatal("historical projection invented absent fields or lost retained values")
		}
	}
	instances.listItems, instances.listTotal = nil, 0
	response, err := NewListInstancesLogic(context.Background(), &svc.ServiceContext{InstanceRepo: instances}).ListInstances(&job.ListInstancesRequest{Page: 1, PageSize: 20})
	if err != nil || response.Items == nil || len(response.Items) != 0 || response.Total != 0 {
		t.Fatal("empty history did not return a non-nil empty list")
	}
	cause := errors.New("synthetic history read failure")
	instances.err = cause
	if response, err := NewListInstancesLogic(context.Background(), &svc.ServiceContext{InstanceRepo: instances}).ListInstances(&job.ListInstancesRequest{Page: 1, PageSize: 20}); response != nil || !errors.Is(err, cause) {
		t.Fatal("history query failure was hidden as an empty list")
	}
}

func TestGetTaskPreservesSelectedDataAndClassifiesFailures(t *testing.T) {
	for _, in := range []*job.GetTaskRequest{nil, {}, {Id: -1}, {Id: 1, DataKind: "both"}, {Id: 1, DataKind: "Input"}} {
		if response, err := NewGetTaskLogic(context.Background(), &svc.ServiceContext{}).GetTask(in); response != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid task detail input reached a repository")
		}
	}
	data, summary := `{"id":9223372036854775807}`, "previous attempt"
	tasks := &executionTasksStub{detail: &repository.TaskDetail{TaskSummary: repository.TaskSummary{ID: 23, InstanceID: 17,
		TaskType: model.TaskTypeShard, Status: model.TaskRunning, AttemptNo: 3, UsedAttempts: 2, ResultSummary: &summary}, Data: &data}}
	logic := NewGetTaskLogic(context.Background(), &svc.ServiceContext{TaskRepo: tasks})
	for _, kind := range []string{"input", "result"} {
		response, err := logic.GetTask(&job.GetTaskRequest{Id: 23, DataKind: kind})
		if err != nil || response.Task.Id != 23 || response.Task.InstanceId != 17 || response.Task.Status != int32(model.TaskRunning) ||
			response.Task.GetResultSummary() != summary || response.Data == nil || *response.Data != data ||
			tasks.detailID != 23 || string(tasks.selection) != kind {
			t.Fatal("task detail changed selected raw JSON or attempt metadata")
		}
	}
	tasks.detail.Data = nil
	response, err := logic.GetTask(&job.GetTaskRequest{Id: 23})
	if err != nil || response.Data != nil || tasks.selection != repository.TaskMetadataOnly {
		t.Fatal("metadata-only detail invented a document")
	}
	for _, failure := range []error{repository.ErrTaskNotFound, repository.ErrTaskDataTooLarge, context.DeadlineExceeded, errors.New("synthetic detail failure"), nil} {
		tasks.detail, tasks.err = nil, failure
		response, err := logic.GetTask(&job.GetTaskRequest{Id: 23, DataKind: "result"})
		if response != nil {
			t.Fatal("failed task lookup returned a successful detail")
		}
		switch {
		case errors.Is(failure, repository.ErrTaskNotFound):
			requireSubcode(t, err, jobsubcode.TaskNotFound)
		case errors.Is(failure, repository.ErrTaskDataTooLarge):
			requireSubcode(t, err, jobsubcode.TaskDataTooLarge)
		case failure == nil:
			if status.Code(err) != codes.Internal {
				t.Fatal("nil record success was disguised as a missing Task")
			}
		default:
			if !errors.Is(err, failure) {
				t.Fatal("task query lost its technical cause")
			}
		}
	}
}

func TestGetTaskBoundsCompleteResponseWithoutTruncation(t *testing.T) {
	data := `"` + strings.Repeat("x", repository.TaskDetailDataLimit-2) + `"`
	summary := strings.Repeat("s", taskDetailResponseLimit-len(data)-200)
	tasks := &executionTasksStub{detail: &repository.TaskDetail{
		TaskSummary: repository.TaskSummary{ID: 1, InstanceID: 2, ResultSummary: &summary}, Data: &data}}
	logic := NewGetTaskLogic(context.Background(), &svc.ServiceContext{TaskRepo: tasks})
	response, err := logic.GetTask(&job.GetTaskRequest{Id: 1, DataKind: "result"})
	if err != nil {
		t.Fatal(err)
	}
	summary += strings.Repeat("s", taskDetailResponseLimit-proto.Size(response))
	response, err = logic.GetTask(&job.GetTaskRequest{Id: 1, DataKind: "result"})
	if err != nil || proto.Size(response) != taskDetailResponseLimit || response.GetData() != data || response.Task.GetResultSummary() != summary {
		t.Fatalf("exact complete response boundary was rejected or truncated: err=%v", err)
	}
	summary += "s"
	response, err = logic.GetTask(&job.GetTaskRequest{Id: 1, DataKind: "result"})
	if response != nil {
		t.Fatal("oversized response was returned or truncated")
	}
	requireSubcode(t, err, jobsubcode.TaskDataTooLarge)
}

func TestInstanceShardStatisticsIncludeCancellationWithoutInventingCompletion(t *testing.T) {
	instances := &executionInstanceStub{row: &model.JobInstance{ID: 9, Status: model.InstanceRunning,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ExecutionMode: model.ExecutionModeSharded}}}
	tasks := &executionTasksStub{counts: repository.ShardCounts{Total: 6, Succeeded: 2, Failed: 1, Canceled: 1}}
	logic := NewGetInstanceLogic(context.Background(), &svc.ServiceContext{InstanceRepo: instances, TaskRepo: tasks})
	response, err := logic.GetInstance(&job.GetInstanceRequest{Id: 9})
	if err != nil || tasks.countCalls != 1 || tasks.instanceID != 9 || response.Instance.ShardStats == nil {
		t.Fatalf("shard statistics query failed: err=%v", err)
	}
	stats := response.Instance.ShardStats
	if stats.Total != 6 || stats.Succeeded != 2 || stats.Failed != 1 || stats.Canceled != 1 || stats.Unfinished != 2 {
		t.Fatal("canceled shards were counted as failed or unfinished")
	}
	tasks.counts = repository.ShardCounts{}
	response, err = logic.GetInstance(&job.GetInstanceRequest{Id: 9})
	if err != nil || response.Instance.ShardStats == nil || response.Instance.Status != int32(model.InstanceRunning) || response.Instance.ShardStats.Total != 0 {
		t.Fatal("zero stored shards were interpreted as completion or no statistics")
	}
	instances.row.ExecutionConfig.ExecutionMode = model.ExecutionModeNormal
	calls := tasks.countCalls
	response, err = logic.GetInstance(&job.GetInstanceRequest{Id: 9})
	if err != nil || response.Instance.ShardStats != nil || tasks.countCalls != calls {
		t.Fatal("ordinary detail queried or returned shard statistics")
	}
	instances.row.ExecutionConfig.ExecutionMode = model.ExecutionModeSharded
	cause := errors.New("synthetic statistics failure")
	tasks.err = cause
	if response, err := logic.GetInstance(&job.GetInstanceRequest{Id: 9}); response != nil || !errors.Is(err, cause) {
		t.Fatal("failed statistics query became a zero count")
	}
	tasks.err, tasks.counts = nil, repository.ShardCounts{Total: 1, Succeeded: 2}
	if response, err := logic.GetInstance(&job.GetInstanceRequest{Id: 9}); response != nil || status.Code(err) != codes.Internal {
		t.Fatal("invalid statistics produced a negative unfinished count")
	}
}
