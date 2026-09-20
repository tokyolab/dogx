package department

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	commonresponse "github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type departmentHandlerRPCStub struct {
	systemclient.System
	called string
	err    error
}

func (s *departmentHandlerRPCStub) CreateDepartment(context.Context, *systemclient.CreateDepartmentRequest, ...grpc.CallOption) (*systemclient.CreateDepartmentResponse, error) {
	s.called = "CreateDepartment"
	return &systemclient.CreateDepartmentResponse{Id: 1}, s.err
}

func (s *departmentHandlerRPCStub) UpdateDepartment(context.Context, *systemclient.UpdateDepartmentRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "UpdateDepartment"
	return &systemclient.EmptyResponse{}, s.err
}

func (s *departmentHandlerRPCStub) UpdateDepartmentStatus(context.Context, *systemclient.UpdateDepartmentStatusRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "UpdateDepartmentStatus"
	return &systemclient.EmptyResponse{}, s.err
}

func (s *departmentHandlerRPCStub) DeleteDepartment(context.Context, *systemclient.DeleteDepartmentRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "DeleteDepartment"
	return &systemclient.EmptyResponse{}, s.err
}

func (s *departmentHandlerRPCStub) GetDepartment(context.Context, *systemclient.GetDepartmentRequest, ...grpc.CallOption) (*systemclient.GetDepartmentResponse, error) {
	s.called = "GetDepartment"
	return &systemclient.GetDepartmentResponse{Department: &systemclient.DepartmentInfo{Id: 1, Name: "部门"}}, s.err
}

func (s *departmentHandlerRPCStub) ListDepartments(context.Context, *systemclient.ListDepartmentsRequest, ...grpc.CallOption) (*systemclient.ListDepartmentsResponse, error) {
	s.called = "ListDepartments"
	return &systemclient.ListDepartmentsResponse{}, s.err
}

var _ systemclient.System = (*departmentHandlerRPCStub)(nil)

func TestDepartmentHandlersRejectMalformedAndInvalidRequestsBeforeRPC(t *testing.T) {
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	httpx.SetValidator(requestvalidator.New())
	t.Cleanup(func() {
		httpx.SetOkHandler(nil)
		httpx.SetErrorHandlerCtx(nil)
		httpx.SetValidator(nil)
	})
	tests := []struct {
		name    string
		path    string
		body    string
		handler func(*svc.ServiceContext) http.HandlerFunc
	}{
		{"create malformed json", "/department/create", `{"name":`, CreateDepartmentHandler},
		{"create blank name", "/department/create", `{"name":" ","status":1}`, CreateDepartmentHandler},
		{"create invalid parent", "/department/create", `{"parentId":-1,"name":"部门","status":1}`, CreateDepartmentHandler},
		{"create invalid status", "/department/create", `{"name":"部门","status":2}`, CreateDepartmentHandler},
		{"create long name", "/department/create", `{"name":"` + strings.Repeat("部", 129) + `","status":1}`, CreateDepartmentHandler},
		{"update malformed json", "/department/update", `{"id":`, UpdateDepartmentHandler},
		{"update invalid id", "/department/update", `{"id":0,"name":"部门"}`, UpdateDepartmentHandler},
		{"update blank name", "/department/update", `{"id":1,"name":" "}`, UpdateDepartmentHandler},
		{"status malformed json", "/department/status/update", `{"id":`, UpdateDepartmentStatusHandler},
		{"status invalid value", "/department/status/update", `{"id":1,"status":2}`, UpdateDepartmentStatusHandler},
		{"delete malformed json", "/department/delete", `{"id":`, DeleteDepartmentHandler},
		{"delete invalid id", "/department/delete", `{"id":0}`, DeleteDepartmentHandler},
		{"get malformed json", "/department/get", `{"id":`, GetDepartmentHandler},
		{"get invalid id", "/department/get", `{"id":0}`, GetDepartmentHandler},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rpc := &departmentHandlerRPCStub{}
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			test.handler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || rpc.called != "" {
				t.Fatalf("invalid request reached RPC: status=%d body=%s called=%s", recorder.Code, recorder.Body.String(), rpc.called)
			}
		})
	}
}

func TestDepartmentHandlersForwardValidRequests(t *testing.T) {
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	httpx.SetValidator(requestvalidator.New())
	t.Cleanup(func() {
		httpx.SetOkHandler(nil)
		httpx.SetErrorHandlerCtx(nil)
		httpx.SetValidator(nil)
	})
	tests := []struct {
		name    string
		path    string
		body    string
		handler func(*svc.ServiceContext) http.HandlerFunc
		called  string
	}{
		{"create", "/department/create", `{"parentId":0,"name":"研发部","sort":1,"remark":"备注","status":1}`, CreateDepartmentHandler, "CreateDepartment"},
		{"update", "/department/update", `{"id":1,"parentId":0,"name":"研发部","sort":1,"remark":"备注"}`, UpdateDepartmentHandler, "UpdateDepartment"},
		{"status", "/department/status/update", `{"id":1,"status":0}`, UpdateDepartmentStatusHandler, "UpdateDepartmentStatus"},
		{"delete", "/department/delete", `{"id":1}`, DeleteDepartmentHandler, "DeleteDepartment"},
		{"get", "/department/get", `{"id":1}`, GetDepartmentHandler, "GetDepartment"},
		{"list", "/department/list", "", ListDepartmentsHandler, "ListDepartments"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rpc := &departmentHandlerRPCStub{}
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			if test.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			recorder := httptest.NewRecorder()
			test.handler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)
			if recorder.Code != http.StatusOK || rpc.called != test.called {
				t.Fatalf("valid request failed: status=%d body=%s called=%s want=%s", recorder.Code, recorder.Body.String(), rpc.called, test.called)
			}
		})
	}
}

func TestDepartmentHandlersReturnRPCFailures(t *testing.T) {
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	httpx.SetValidator(requestvalidator.New())
	t.Cleanup(func() {
		httpx.SetOkHandler(nil)
		httpx.SetErrorHandlerCtx(nil)
		httpx.SetValidator(nil)
	})
	errRPC := status.Error(codes.Unavailable, "RPC unavailable")
	tests := []struct {
		name    string
		path    string
		body    string
		handler func(*svc.ServiceContext) http.HandlerFunc
	}{
		{"create", "/department/create", `{"parentId":0,"name":"研发部","sort":0,"status":1}`, CreateDepartmentHandler},
		{"update", "/department/update", `{"id":1,"parentId":0,"name":"研发部","sort":0}`, UpdateDepartmentHandler},
		{"status", "/department/status/update", `{"id":1,"status":0}`, UpdateDepartmentStatusHandler},
		{"delete", "/department/delete", `{"id":1}`, DeleteDepartmentHandler},
		{"get", "/department/get", `{"id":1}`, GetDepartmentHandler},
		{"list", "/department/list", "", ListDepartmentsHandler},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rpc := &departmentHandlerRPCStub{err: errRPC}
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			if test.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			recorder := httptest.NewRecorder()
			test.handler(&svc.ServiceContext{SystemRpc: rpc}).ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || rpc.called == "" {
				t.Fatalf("RPC failure response: status=%d body=%s called=%s", recorder.Code, recorder.Body.String(), rpc.called)
			}
		})
	}
}
