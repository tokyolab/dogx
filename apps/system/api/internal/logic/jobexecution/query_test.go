package jobexecution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetInstanceAPIKeepsFrozenConfigAndJSONNulls(t *testing.T) {
	client := &executionRPCStub{get: &jobclient.GetInstanceResponse{Instance: &jobclient.InstanceInfo{
		Id: 9, JobId: 7, JobName: "frozen", ExecutorGroupId: 3, TriggerType: 3, ParamsOverridden: true, Status: 1,
		ExecutionConfig: &jobclient.ExecutionConfigInfo{SchemaVersion: 1, ProcessorName: "system.frozen", ExecutionMode: 1, Params: "  frozen params  ", MaxRetries: 0},
		CreatedAt:       "2026-11-05T08:01:02.123456+08:00", UpdatedAt: "2026-11-05T08:01:02.123456+08:00",
	}}}
	logic := NewGetInstanceLogic(context.Background(), &svc.ServiceContext{JobRpc: client})
	result, err := logic.GetInstance(&types.IDReq{Id: 9})
	if err != nil || client.getRequest.Id != 9 || result.ExecutionConfig.Params != "  frozen params  " || result.ExecutionConfig.MaxRetries != 0 || !result.ParamsOverridden {
		t.Fatalf("detail adaptation failed: %v", err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"scheduledAt", "requestId", "triggeredBy", "startedAt", "deadlineAt", "finishedAt", "endReason", "resultSummary"} {
		if string(fields[name]) != "null" {
			t.Fatalf("absent %s was not JSON null", name)
		}
	}
	when, executor, empty := "2026-11-05T08:01:02.123456+08:00", "synthetic-request-identity", ""
	client.get.Instance.StartedAt, client.get.Instance.RequestId, client.get.Instance.ResultSummary = &when, &executor, &empty
	result, err = logic.GetInstance(&types.IDReq{Id: 9})
	if err != nil || result.StartedAt == nil || *result.StartedAt != when || result.RequestId == nil || *result.RequestId != executor || result.ResultSummary == nil || *result.ResultSummary != "" {
		t.Fatal("present optional execution values were changed")
	}
}

func TestListTasksAPIHasNoBusinessDataAndUsesEmptyArray(t *testing.T) {
	client := &executionRPCStub{list: &jobclient.ListInstanceTasksResponse{}}
	logic := NewListInstanceTasksLogic(context.Background(), &svc.ServiceContext{JobRpc: client})
	request := &types.JobTaskListReq{InstanceId: 9, PageReq: types.PageReq{Page: 2, PageSize: 200}}
	result, err := logic.ListInstanceTasks(request)
	if err != nil || result.Items == nil || client.listRequest.InstanceId != 9 || client.listRequest.Page != 2 || client.listRequest.PageSize != 200 {
		t.Fatalf("task pagination or empty list mapping failed: %v", err)
	}
	encoded, _ := json.Marshal(result)
	if string(encoded) != `{"total":0,"items":[]}` {
		t.Fatal("empty tasks are not an empty JSON array")
	}
	previous, empty, when, executor, address := "previous attempt summary", "", "2026-11-05T08:00:00Z", "test-executor-id", "127.0.0.1:9090"
	client.list = &jobclient.ListInstanceTasksResponse{Total: 4, Items: []*jobclient.TaskListItem{{Id: 3, InstanceId: 9, TaskType: 1, Status: 2, AttemptNo: 2, UsedAttempts: 1,
		AttemptRegisteredAt: &when, NextDispatchAt: &when, ExecutorInstanceId: &executor, ExecutorAddress: &address,
		ResultSummary: &previous, EndReason: &empty, StartedAt: &when, CreatedAt: when, UpdatedAt: when}}}
	result, err = logic.ListInstanceTasks(request)
	if err != nil || result.Total != 4 || len(result.Items) != 1 || result.Items[0].FinishedAt != nil || result.Items[0].AttemptNo != 2 ||
		result.Items[0].ResultSummary == nil || *result.Items[0].ResultSummary != previous || result.Items[0].EndReason == nil ||
		result.Items[0].ExecutorInstanceId == nil || *result.Items[0].ExecutorInstanceId != executor || result.Items[0].ExecutorAddress == nil || *result.Items[0].ExecutorAddress != address {
		t.Fatal("task facts were lost or interpreted as completion")
	}
	encoded, _ = json.Marshal(result)
	if strings.Contains(string(encoded), "resultData") || strings.Contains(string(encoded), "result_data") {
		t.Fatal("task list exposed business data")
	}
}

func TestExecutionAPIRejectsIncompleteResponsesAndPreservesErrors(t *testing.T) {
	client := &executionRPCStub{}
	s := &svc.ServiceContext{JobRpc: client}
	get, list := NewGetInstanceLogic(context.Background(), s), NewListInstanceTasksLogic(context.Background(), s)
	if _, err := get.GetInstance(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal("nil detail request accepted")
	}
	if _, err := list.ListInstanceTasks(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal("nil page request accepted")
	}
	for _, response := range []*jobclient.GetInstanceResponse{nil, {}, {Instance: &jobclient.InstanceInfo{}}} {
		client.get = response
		if _, err := get.GetInstance(&types.IDReq{Id: 9}); status.Code(err) != codes.Internal {
			t.Fatal("incomplete detail response accepted")
		}
	}
	for _, response := range []*jobclient.ListInstanceTasksResponse{nil, {Items: []*jobclient.TaskListItem{nil}}} {
		client.list = response
		if _, err := list.ListInstanceTasks(&types.JobTaskListReq{InstanceId: 9, PageReq: types.PageReq{Page: 1, PageSize: 1}}); status.Code(err) != codes.Internal {
			t.Fatal("incomplete list response accepted")
		}
	}
	cause := errors.New("synthetic RPC failure")
	client.err = cause
	if _, err := get.GetInstance(&types.IDReq{Id: 9}); !errors.Is(err, cause) {
		t.Fatal("detail RPC failure was replaced")
	}
	if _, err := list.ListInstanceTasks(&types.JobTaskListReq{InstanceId: 9, PageReq: types.PageReq{Page: 1, PageSize: 1}}); !errors.Is(err, cause) {
		t.Fatal("list RPC failure was replaced")
	}
}

type executionRPCStub struct {
	jobclient.Job
	get         *jobclient.GetInstanceResponse
	stop        *jobclient.StopInstanceResponse
	list        *jobclient.ListInstanceTasksResponse
	err         error
	getRequest  *jobclient.GetInstanceRequest
	stopRequest *jobclient.StopInstanceRequest
	listRequest *jobclient.ListInstanceTasksRequest
}

func (s *executionRPCStub) GetInstance(_ context.Context, in *jobclient.GetInstanceRequest, _ ...grpc.CallOption) (*jobclient.GetInstanceResponse, error) {
	s.getRequest = in
	return s.get, s.err
}

func (s *executionRPCStub) StopInstance(_ context.Context, in *jobclient.StopInstanceRequest, _ ...grpc.CallOption) (*jobclient.StopInstanceResponse, error) {
	s.stopRequest = in
	return s.stop, s.err
}

func (s *executionRPCStub) ListInstanceTasks(_ context.Context, in *jobclient.ListInstanceTasksRequest, _ ...grpc.CallOption) (*jobclient.ListInstanceTasksResponse, error) {
	s.listRequest = in
	return s.list, s.err
}
