package jobexecution

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	"github.com/tokyolab/dogx/pkg/response"
	"github.com/tokyolab/dogx/pkg/subcode"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
)

func TestExecutionHandlersValidateTheHTTPBoundary(t *testing.T) {
	httpx.SetValidator(requestvalidator.New())
	httpx.SetErrorHandlerCtx(response.HandleError)
	httpx.SetOkHandler(response.HandleSuccess)
	t.Cleanup(func() { httpx.SetValidator(nil); httpx.SetErrorHandlerCtx(nil); httpx.SetOkHandler(nil) })
	for _, test := range []struct {
		name, body        string
		list, stop, valid bool
	}{
		{name: "detail", body: `{"id":7}`, valid: true},
		{name: "missing ID", body: `{}`},
		{name: "null ID", body: `{"id":null}`},
		{name: "zero ID", body: `{"id":0}`},
		{name: "stop", body: `{"id":7}`, stop: true, valid: true},
		{name: "stop missing ID", body: `{}`, stop: true},
		{name: "stop null ID", body: `{"id":null}`, stop: true},
		{name: "stop zero ID", body: `{"id":0}`, stop: true},
		{name: "stop negative ID", body: `{"id":-1}`, stop: true},
		{name: "stop invalid JSON", body: `{"id":`, stop: true},
		{name: "page", body: `{"instanceId":7,"page":2,"pageSize":200}`, list: true, valid: true},
		{name: "missing parent", body: `{"page":1,"pageSize":20}`, list: true},
		{name: "zero page", body: `{"instanceId":7,"page":0,"pageSize":20}`, list: true},
		{name: "oversized page", body: `{"instanceId":7,"page":1,"pageSize":201}`, list: true},
		{name: "invalid JSON", body: `{"instanceId":`, list: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stub := &executionHandlerRPCStub{}
			s := &svc.ServiceContext{JobRpc: stub}
			handler := GetInstanceHandler(s)
			path := "/job/instance/get"
			if test.list {
				handler, path = ListInstanceTasksHandler(s), "/job/task/list"
			} else if test.stop {
				handler, path = StopInstanceHandler(s), "/job/instance/stop"
			}
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			handler(recorder, request)
			var result response.Body
			if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if test.valid {
				if recorder.Code != http.StatusOK || result.Code != 0 || stub.calls != 1 {
					t.Fatal("valid execution request did not reach RPC")
				}
				if test.list && (stub.list.InstanceId != 7 || stub.list.Page != 2 || stub.list.PageSize != 200) {
					t.Fatal("HTTP pagination changed before RPC")
				}
				if test.stop && (stub.stop == nil || stub.stop.Id != 7) {
					t.Fatal("HTTP stop ID changed before RPC")
				}
			} else if recorder.Code != http.StatusBadRequest || result.Subcode != subcode.InvalidRequest || stub.calls != 0 {
				t.Fatal("invalid execution request reached RPC or lost its error contract")
			}
		})
	}
}

type executionHandlerRPCStub struct {
	jobclient.Job
	calls int
	list  *jobclient.ListInstanceTasksRequest
	stop  *jobclient.StopInstanceRequest
}

func (s *executionHandlerRPCStub) GetInstance(context.Context, *jobclient.GetInstanceRequest, ...grpc.CallOption) (*jobclient.GetInstanceResponse, error) {
	s.calls++
	return &jobclient.GetInstanceResponse{Instance: &jobclient.InstanceInfo{Id: 7, ExecutionConfig: &jobclient.ExecutionConfigInfo{SchemaVersion: 1}}}, nil
}

func (s *executionHandlerRPCStub) ListInstanceTasks(_ context.Context, req *jobclient.ListInstanceTasksRequest, _ ...grpc.CallOption) (*jobclient.ListInstanceTasksResponse, error) {
	s.calls++
	s.list = req
	return &jobclient.ListInstanceTasksResponse{}, nil
}

func (s *executionHandlerRPCStub) StopInstance(_ context.Context, req *jobclient.StopInstanceRequest, _ ...grpc.CallOption) (*jobclient.StopInstanceResponse, error) {
	s.calls++
	s.stop = req
	reason := "manual_stop"
	return &jobclient.StopInstanceResponse{InstanceId: req.Id, Status: 6, EndReason: &reason}, nil
}
