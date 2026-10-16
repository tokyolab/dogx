package job

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/pkg/subcode"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestJobHandlersHTTPBoundary(t *testing.T) {
	setHTTPResponses(t)
	for _, test := range []struct {
		method, path, body, invalid, data string
		handler                           func(*svc.ServiceContext) http.HandlerFunc
		request                           proto.Message
	}{
		{
			method: "ListJobs", path: "/job/list", handler: ListJobsHandler,
			body: `{"page":1,"pageSize":20}`, invalid: `{"page":0,"pageSize":20}`,
			request: &jobclient.ListJobsRequest{Page: 1, PageSize: 20}, data: `{"items":[],"total":0}`,
		},
		{
			method: "GetJob", path: "/job/get", handler: GetJobHandler,
			body: `{"id":7}`, invalid: `{"id":0}`, request: &jobclient.GetJobRequest{Id: 7},
			data: `{"id":7,"name":"","executorGroupId":0,"executorGroupName":null,"processorName":"","executionMode":0,"cronExpr":"","status":0,"nextTriggerAt":null,"createdAt":"","updatedAt":"","remark":"","postEnabled":false,"params":"","misfirePolicy":0,"blockPolicy":0,"maxRetries":0,"retryIntervalSeconds":0,"timeoutSeconds":0}`,
		},
		{
			method: "CreateJob", path: "/job/create", handler: CreateJobHandler,
			body:    `{"name":"job","executorGroupId":3,"processorName":"Handler","cronExpr":"0 * * * * *","timeoutSeconds":900}`,
			invalid: `{"name":"job","executorGroupId":0,"processorName":"Handler","cronExpr":"0 * * * * *","timeoutSeconds":900}`,
			request: &jobclient.CreateJobRequest{Config: &jobclient.JobInput{Name: "job", ExecutorGroupId: 3, ProcessorName: "Handler", CronExpr: "0 * * * * *", TimeoutSeconds: 900}}, data: `{"id":7}`,
		},
		{
			method: "UpdateJob", path: "/job/update", handler: UpdateJobHandler,
			body:    `{"id":7,"name":"job","executorGroupId":3,"processorName":"Handler","executionMode":1,"postEnabled":false,"cronExpr":"0 * * * * *","misfirePolicy":0,"blockPolicy":0,"maxRetries":0,"retryIntervalSeconds":10,"timeoutSeconds":900}`,
			invalid: `{"id":7,"name":"job","executorGroupId":3,"processorName":"Handler","executionMode":1,"cronExpr":"0 * * * * *","misfirePolicy":0,"blockPolicy":0,"maxRetries":0,"retryIntervalSeconds":10,"timeoutSeconds":900}`,
			request: &jobclient.UpdateJobRequest{Id: 7, Config: &jobclient.JobInput{Name: "job", ExecutorGroupId: 3, ProcessorName: "Handler", ExecutionMode: proto.Int32(1), PostEnabled: proto.Bool(false), CronExpr: "0 * * * * *", MisfirePolicy: proto.Int32(0), BlockPolicy: proto.Int32(0), MaxRetries: proto.Int32(0), RetryIntervalSeconds: proto.Int32(10), TimeoutSeconds: 900}}, data: `{}`,
		},
		{
			method: "UpdateJobStatus", path: "/job/status/update", handler: UpdateJobStatusHandler,
			body: `{"id":7,"status":0}`, invalid: `{"id":7}`, request: &jobclient.UpdateJobStatusRequest{Id: 7, Status: proto.Int32(0)}, data: `{}`,
		},
		{
			method: "DeleteJob", path: "/job/delete", handler: DeleteJobHandler,
			body: `{"id":7}`, invalid: `{"id":0}`, request: &jobclient.DeleteJobRequest{Id: 7}, data: `{}`,
		},
		{
			method: "PreviewJobCron", path: "/job/cron/preview", handler: PreviewJobCronHandler,
			body: `{"cronExpr":"0 * * * * *"}`, invalid: `{"cronExpr":""}`, request: &jobclient.PreviewJobCronRequest{CronExpr: "0 * * * * *"}, data: `{"times":["2026-10-16T12:01:00+08:00"]}`,
		},
		{
			method: "TriggerJob", path: "/job/trigger", handler: TriggerJobHandler,
			body:    `{"id":7,"requestId":"12051a0f-76b8-4d88-897f-15ec600de323","paramsOverridden":false}`,
			invalid: `{"id":7,"requestId":"12051a0f-76b8-4d88-897f-15ec600de323"}`,
			request: &jobclient.TriggerJobRequest{Id: 7, RequestId: "12051a0f-76b8-4d88-897f-15ec600de323", ParamsOverridden: proto.Bool(false), OperatorId: 42}, data: `{"instanceId":9}`,
		},
	} {
		t.Run(test.method, func(t *testing.T) {
			for _, scenario := range []struct {
				name, body            string
				rpcError              error
				wantStatus, wantCalls int
				wantSubcode           string
			}{
				{name: "success", body: test.body, wantStatus: http.StatusOK, wantCalls: 1},
				{name: "invalid field", body: test.invalid, wantStatus: http.StatusBadRequest, wantSubcode: subcode.InvalidRequest},
				{name: "malformed JSON", body: `{"secret.internal":`, wantStatus: http.StatusBadRequest, wantSubcode: subcode.InvalidRequest},
				{name: "RPC unavailable", body: test.body, rpcError: status.Error(codes.Unavailable, "database secret.internal password"), wantStatus: http.StatusServiceUnavailable, wantCalls: 1, wantSubcode: subcode.ServiceUnavailable},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					stub := &handlerJobRPCStub{err: scenario.rpcError}
					request := handlerRequest(scenario.body)
					request.URL.Path = test.path
					ctx := context.WithValue(request.Context(), "userId", int64(42))
					ctx = context.WithValue(ctx, "sessionId", "session")
					ctx = context.WithValue(ctx, "isSuperAdmin", false)
					recorder := httptest.NewRecorder()
					test.handler(&svc.ServiceContext{JobRpc: stub})(recorder, request.WithContext(ctx))
					body := decodedResponse(t, recorder)
					wantCode := uint32(scenario.wantStatus)
					if scenario.wantStatus == http.StatusOK {
						wantCode = 0
					}
					if recorder.Code != scenario.wantStatus || body.Code != wantCode || body.Subcode != scenario.wantSubcode || body.Message == "" || stub.calls != scenario.wantCalls {
						t.Fatalf("status=%d response=%s calls=%d", recorder.Code, recorder.Body.String(), stub.calls)
					}
					if scenario.wantCalls == 1 && (stub.method != test.method || !proto.Equal(stub.request, test.request)) {
						t.Fatalf("RPC method=%s request=%v want=%v", stub.method, stub.request, test.request)
					}
					if scenario.wantCalls == 0 && (stub.method != "" || stub.request != nil) {
						t.Fatalf("parse failure reached RPC: %s %v", stub.method, stub.request)
					}
					if strings.Contains(recorder.Body.String(), "secret.internal") || strings.Contains(recorder.Body.String(), "password") {
						t.Fatalf("internal detail leaked: %s", recorder.Body.String())
					}
					if scenario.wantStatus != http.StatusOK {
						if body.Data != nil {
							t.Fatalf("error data=%v", body.Data)
						}
						return
					}
					var wantData any
					if err := json.Unmarshal([]byte(test.data), &wantData); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(body.Data, wantData) {
						t.Fatalf("data=%#v want=%#v", body.Data, wantData)
					}
				})
			}
		})
	}
}
