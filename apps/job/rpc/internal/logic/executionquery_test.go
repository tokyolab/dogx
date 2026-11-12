package logic

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	jobsubcode "github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExecutionQueriesRejectInvalidInputBeforeReading(t *testing.T) {
	s := &svc.ServiceContext{}
	for _, in := range []*job.GetInstanceRequest{nil, {}, {Id: -1}} {
		_, err := NewGetInstanceLogic(context.Background(), s).GetInstance(in)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid detail input classification=%v", status.Code(err))
		}
	}
	for _, in := range []*job.ListInstanceTasksRequest{
		nil, {}, {InstanceId: 1, Page: 0, PageSize: 1}, {InstanceId: 1, Page: 1, PageSize: 0},
		{InstanceId: 1, Page: 1, PageSize: 201}, {InstanceId: 1, Page: math.MaxInt64, PageSize: 2},
	} {
		_, err := NewListInstanceTasksLogic(context.Background(), s).ListInstanceTasks(in)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid list input classification=%v", status.Code(err))
		}
	}
}

func TestGetInstanceProjectsFrozenAndNullableFacts(t *testing.T) {
	at := time.Date(2026, 11, 5, 8, 1, 2, 123456000, time.FixedZone("test", 8*3600))
	row := &model.JobInstance{ID: 9, JobID: 7, JobName: "frozen name", ExecutorGroupID: 3,
		TriggerType: model.TriggerManual, ParamsOverridden: true, Status: model.InstanceWaiting,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "system.frozen", ExecutionMode: model.ExecutionModeNormal,
			Params: "  frozen\nparameters  ", MaxRetries: 0, RetryIntervalSeconds: 10, TimeoutSeconds: 60}, CreatedAt: at, UpdatedAt: at}
	instances := &executionInstanceStub{row: row}
	s := &svc.ServiceContext{InstanceRepo: instances}
	// No Job or group repository is installed: historical detail is self-contained.
	result, err := NewGetInstanceLogic(context.Background(), s).GetInstance(&job.GetInstanceRequest{Id: 9})
	if err != nil || instances.id != 9 || instances.calls != 1 {
		t.Fatalf("instance lookup failed: %v", err)
	}
	item := result.Instance
	if item.JobName != row.JobName || item.JobId != row.JobID || item.ExecutorGroupId != row.ExecutorGroupID || item.TriggerType != int32(row.TriggerType) ||
		!item.ParamsOverridden || item.ExecutionConfig.Params != row.ExecutionConfig.Params || item.ExecutionConfig.MaxRetries != 0 ||
		item.ExecutionConfig.PostEnabled || item.ExecutionConfig.ProcessorName != "system.frozen" || item.ExecutionConfig.TimeoutSeconds != 60 ||
		item.StartedAt != nil || item.DeadlineAt != nil || item.FinishedAt != nil || item.RequestId != nil || item.TriggeredBy != nil ||
		item.ScheduledAt != nil || item.EndReason != nil || item.ResultSummary != nil || item.CreatedAt != at.Format(time.RFC3339Nano) {
		t.Fatal("projection changed the frozen snapshot or manufactured absent execution facts")
	}
	requestID, operatorID, summary, reason := uuid.New(), int64(11), "", "success"
	row.RequestID, row.TriggeredBy, row.StartedAt, row.DeadlineAt, row.FinishedAt = &requestID, &operatorID, &at, &at, &at
	row.ScheduledAt, row.ResultSummary, row.EndReason = &at, &summary, &reason
	result, err = NewGetInstanceLogic(context.Background(), s).GetInstance(&job.GetInstanceRequest{Id: 9})
	if err != nil || result.Instance.GetRequestId() != requestID.String() || result.Instance.GetTriggeredBy() != operatorID ||
		result.Instance.GetStartedAt() != at.Format(time.RFC3339Nano) || result.Instance.GetDeadlineAt() != at.Format(time.RFC3339Nano) ||
		result.Instance.GetFinishedAt() != at.Format(time.RFC3339Nano) || result.Instance.GetScheduledAt() != at.Format(time.RFC3339Nano) ||
		result.Instance.ResultSummary == nil || *result.Instance.ResultSummary != "" || result.Instance.GetEndReason() != reason {
		t.Fatal("present values or an explicitly empty summary lost presence")
	}
}

func TestInstanceQueryFailuresDoNotBecomeAbsenceOrEmptyList(t *testing.T) {
	cause := errors.New("synthetic database failure")
	for _, readErr := range []error{repository.ErrInstanceNotFound, cause, context.DeadlineExceeded, nil} {
		instances := &executionInstanceStub{err: readErr}
		s := &svc.ServiceContext{InstanceRepo: instances}
		_, err := NewGetInstanceLogic(context.Background(), s).GetInstance(&job.GetInstanceRequest{Id: 9})
		switch {
		case errors.Is(readErr, repository.ErrInstanceNotFound):
			requireSubcode(t, err, jobsubcode.InstanceNotFound)
		case readErr == nil:
			if status.Code(err) != codes.Internal {
				t.Fatal("nil success was disguised as absence")
			}
		default:
			if !errors.Is(err, readErr) {
				t.Fatal("technical read failure lost its cause")
			}
		}
	}
	for _, readErr := range []error{cause, context.DeadlineExceeded, nil} {
		instances := &executionInstanceStub{err: readErr}
		s := &svc.ServiceContext{InstanceRepo: instances}
		_, err := NewListInstanceTasksLogic(context.Background(), s).ListInstanceTasks(&job.ListInstanceTasksRequest{InstanceId: 9, Page: 1, PageSize: 20})
		if readErr == nil {
			requireSubcode(t, err, jobsubcode.InstanceNotFound)
		} else if !errors.Is(err, readErr) {
			t.Fatal("failed existence query was disguised as absence")
		}
		if instances.existsCalls != 1 || instances.findCalls != 0 {
			t.Fatal("task list loaded an execution snapshot instead of checking existence")
		}
	}
}

func TestListInstanceTasksPreservesPagingAndOptionalFacts(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Microsecond)
	id, address, summary, reason := uuid.New(), "127.0.0.1:9090", "previous attempt", ""
	// The parent snapshot is intentionally unavailable. Task facts only need an
	// existence result and must not depend on decoding a frozen configuration.
	instances := &executionInstanceStub{exists: true}
	tasks := &executionTasksStub{items: []repository.TaskSummary{
		{ID: 3, InstanceID: 9, TaskType: model.TaskTypeOrdinary, Status: model.TaskRunning, AttemptNo: 2, UsedAttempts: 1,
			AttemptRegisteredAt: &at, NextDispatchAt: &at, ExecutorInstanceID: &id, ExecutorAddress: &address,
			ResultSummary: &summary, EndReason: &reason, StartedAt: &at, CreatedAt: at, UpdatedAt: at},
		{ID: 5, InstanceID: 9, TaskType: model.TaskTypeShard, Status: model.TaskWaiting, CreatedAt: at, UpdatedAt: at},
	}, total: 8}
	s := &svc.ServiceContext{InstanceRepo: instances, TaskRepo: tasks}
	in := &job.ListInstanceTasksRequest{InstanceId: 9, Page: 2, PageSize: 200}
	result, err := NewListInstanceTasksLogic(context.Background(), s).ListInstanceTasks(in)
	if err != nil || tasks.calls != 1 || instances.existsCalls != 1 || instances.findCalls != 0 || tasks.instanceID != 9 || tasks.page != 2 || tasks.pageSize != 200 ||
		len(result.Items) != 2 || result.Total != 8 || result.Items[0].Id != 3 || result.Items[1].Id != 5 {
		t.Fatalf("bounded task query failed: %v", err)
	}
	first, second := result.Items[0], result.Items[1]
	if first.AttemptNo != 2 || first.UsedAttempts != 1 || first.GetExecutorInstanceId() != id.String() || first.GetExecutorAddress() != address ||
		first.GetAttemptRegisteredAt() != at.Format(time.RFC3339Nano) || first.GetNextDispatchAt() != at.Format(time.RFC3339Nano) ||
		first.GetStartedAt() != at.Format(time.RFC3339Nano) || first.GetResultSummary() != summary || first.EndReason == nil || first.FinishedAt != nil ||
		second.ExecutorInstanceId != nil || second.ExecutorAddress != nil || second.StartedAt != nil || second.ResultSummary != nil {
		t.Fatal("task projection changed attempt facts or nullable fields")
	}
	tasks.items, tasks.total = nil, 0
	result, err = NewListInstanceTasksLogic(context.Background(), s).ListInstanceTasks(in)
	if err != nil || result.Items == nil || len(result.Items) != 0 || result.Total != 0 {
		t.Fatal("existing parent without tasks did not return an empty list")
	}
	cause := errors.New("synthetic page failure")
	tasks.err = cause
	if _, err := NewListInstanceTasksLogic(context.Background(), s).ListInstanceTasks(in); !errors.Is(err, cause) {
		t.Fatal("page failure became an empty list")
	}
}

func TestGetInstanceRejectsUninterpretableSnapshotsWithoutDefaults(t *testing.T) {
	for _, config := range []model.ExecutionConfig{
		{},
		{SchemaVersion: 2, ExecutionMode: model.ExecutionModeNormal},
		{SchemaVersion: 1},
		{SchemaVersion: 1, ExecutionMode: model.ExecutionMode(99)},
	} {
		row := &model.JobInstance{ID: 9, ExecutionConfig: config}
		instances := &executionInstanceStub{row: row}
		response, err := NewGetInstanceLogic(context.Background(), &svc.ServiceContext{InstanceRepo: instances}).GetInstance(&job.GetInstanceRequest{Id: 9})
		if response != nil || status.Code(err) != codes.Internal || row.ExecutionConfig != config {
			t.Fatal("invalid snapshot was returned or repaired with invented defaults")
		}
	}
	for _, mode := range []model.ExecutionMode{model.ExecutionModeNormal, model.ExecutionModeSharded} {
		config := model.ExecutionConfig{SchemaVersion: 1, ExecutionMode: mode, ProcessorName: "test.processor", MaxRetries: 0,
			RetryIntervalSeconds: 10, TimeoutSeconds: 60, PostEnabled: mode == model.ExecutionModeSharded}
		instances := &executionInstanceStub{row: &model.JobInstance{ID: 9, ExecutionConfig: config}}
		response, err := NewGetInstanceLogic(context.Background(), &svc.ServiceContext{InstanceRepo: instances, TaskRepo: &executionTasksStub{}}).GetInstance(&job.GetInstanceRequest{Id: 9})
		if err != nil || response.Instance.ExecutionConfig.ExecutionMode != int32(mode) || response.Instance.ExecutionConfig.MaxRetries != 0 ||
			response.Instance.ExecutionConfig.Params != "" || response.Instance.ExecutionConfig.PostEnabled != config.PostEnabled {
			t.Fatal("valid snapshot values were replaced with defaults")
		}
	}
}

// Unused embedded repository methods panic if these read-only Logic tests call
// a mutation or another query. Each stub and all its overrides stay together.
type executionInstanceStub struct {
	repository.JobInstanceRepository
	row         *model.JobInstance
	err         error
	id          int64
	calls       int
	exists      bool
	findCalls   int
	existsCalls int
	listItems   []repository.InstanceListItem
	listTotal   int64
	listFilter  repository.InstanceFilter
}

func (s *executionInstanceStub) List(_ context.Context, filter repository.InstanceFilter) ([]repository.InstanceListItem, int64, error) {
	s.calls++
	s.listFilter = filter
	return s.listItems, s.listTotal, s.err
}

func (s *executionInstanceStub) FindByID(_ context.Context, id int64) (*model.JobInstance, error) {
	s.id, s.calls = id, s.calls+1
	s.findCalls++
	return s.row, s.err
}

func (s *executionInstanceStub) Exists(_ context.Context, id int64) (bool, error) {
	s.id, s.calls = id, s.calls+1
	s.existsCalls++
	return s.exists, s.err
}

type executionTasksStub struct {
	repository.JobTaskRepository
	items                      []repository.TaskSummary
	total                      int64
	err                        error
	instanceID, page, pageSize int64
	calls                      int
	detail                     *repository.TaskDetail
	detailID                   int64
	selection                  repository.TaskDataSelection
	counts                     repository.ShardCounts
	countCalls                 int
}

func (s *executionTasksStub) FindDetail(_ context.Context, id int64, selection repository.TaskDataSelection) (*repository.TaskDetail, error) {
	s.calls++
	s.detailID, s.selection = id, selection
	return s.detail, s.err
}

func (s *executionTasksStub) CountShards(_ context.Context, id int64) (repository.ShardCounts, error) {
	s.countCalls++
	s.instanceID = id
	return s.counts, s.err
}

func (s *executionTasksStub) ListByInstance(_ context.Context, id, page, pageSize int64) ([]repository.TaskSummary, int64, error) {
	s.instanceID, s.page, s.pageSize, s.calls = id, page, pageSize, s.calls+1
	return s.items, s.total, s.err
}
