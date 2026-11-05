package jobgroup

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	"github.com/tokyolab/dogx/pkg/response"
	"github.com/tokyolab/dogx/pkg/subcode"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func configureHTTP(t *testing.T) {
	t.Helper()
	httpx.SetValidator(requestvalidator.New())
	httpx.SetErrorHandlerCtx(response.HandleError)
	httpx.SetOkHandler(response.HandleSuccess)
	t.Cleanup(func() {
		httpx.SetValidator(nil)
		httpx.SetErrorHandlerCtx(nil)
		httpx.SetOkHandler(nil)
	})
}

func invokeHandler(t *testing.T, handler http.HandlerFunc, path, body string) (*httptest.ResponseRecorder, response.Body) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler(recorder, req)
	var result response.Body
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return recorder, result
}

func TestExecutorGroupHandlersHTTPBoundary(t *testing.T) {
	configureHTTP(t)
	for _, test := range []struct {
		method, path, body, invalid, data string
		handler                           func(*svc.ServiceContext) http.HandlerFunc
		request                           proto.Message
	}{
		{
			method: "CreateExecutorGroup", path: "/job/group/create", handler: CreateExecutorGroupHandler,
			body: `{"name":"Workers","discoveryKey":"/workers/rpc","token":"ignored-client-value"}`, invalid: `{"name":"Workers"}`,
			request: &jobclient.CreateExecutorGroupRequest{Name: "Workers", DiscoveryKey: "/workers/rpc"}, data: `{"id":7}`,
		},
		{
			method: "GetExecutorGroup", path: "/job/group/get", handler: GetExecutorGroupHandler,
			body: `{"id":7}`, invalid: `{"id":0}`, request: &jobclient.GetExecutorGroupRequest{Id: 7},
			data: `{"id":7,"name":"Workers","discoveryKey":"/workers/rpc","token":"detail-only-credential","createdAt":"created","updatedAt":"updated"}`,
		},
		{
			method: "ListExecutorGroups", path: "/job/group/list", handler: ListExecutorGroupsHandler,
			body: `{"page":2,"pageSize":20,"name":"_Exact%"}`, invalid: `{"page":1,"pageSize":201}`,
			request: &jobclient.ListExecutorGroupsRequest{Page: 2, PageSize: 20, Name: "_Exact%"},
			data:    `{"items":[{"id":7,"name":"Workers","discoveryKey":"/workers/rpc","createdAt":"created","updatedAt":"updated"}],"total":1}`,
		},
		{
			method: "DeleteExecutorGroup", path: "/job/group/delete", handler: DeleteExecutorGroupHandler,
			body: `{"id":7}`, invalid: `{"id":0}`, request: &jobclient.DeleteExecutorGroupRequest{Id: 7}, data: `{}`,
		},
	} {
		t.Run(test.method, func(t *testing.T) {
			for _, scenario := range []struct {
				name, body  string
				err         error
				status      int
				calls       int
				wantSubcode string
			}{
				{name: "success", body: test.body, status: http.StatusOK, calls: 1},
				{name: "invalid field", body: test.invalid, status: http.StatusBadRequest, wantSubcode: subcode.InvalidRequest},
				{name: "malformed JSON", body: `{"secret.internal":`, status: http.StatusBadRequest, wantSubcode: subcode.InvalidRequest},
				{name: "RPC unavailable", body: test.body, err: status.Error(codes.Unavailable, "database secret.internal password"), status: http.StatusServiceUnavailable, calls: 1, wantSubcode: subcode.ServiceUnavailable},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					stub := &handlerGroupRPCStub{err: scenario.err}
					recorder, result := invokeHandler(t, test.handler(&svc.ServiceContext{JobRpc: stub}), test.path, scenario.body)
					wantCode := uint32(scenario.status)
					if scenario.status == http.StatusOK {
						wantCode = 0
					}
					if recorder.Code != scenario.status || result.Code != wantCode || result.Subcode != scenario.wantSubcode || result.Message == "" || stub.calls != scenario.calls {
						t.Fatalf("HTTP=%d code=%d subcode=%s calls=%d", recorder.Code, result.Code, result.Subcode, stub.calls)
					}
					if stub.calls != 0 && (stub.method != test.method || !proto.Equal(stub.request, test.request)) {
						t.Fatalf("RPC method=%s request=%v", stub.method, stub.request)
					}
					if strings.Contains(recorder.Body.String(), "secret.internal") || strings.Contains(recorder.Body.String(), "password") {
						t.Fatal("technical response exposed dependency details")
					}
					if scenario.status != http.StatusOK {
						if result.Data != nil {
							t.Fatal("failed response returned success data")
						}
						return
					}
					var expected any
					if err := json.Unmarshal([]byte(test.data), &expected); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(result.Data, expected) {
						t.Fatal("HTTP response did not match its resource contract")
					}
				})
			}
		})
	}
}

func TestCreateGroupUsesUnicodeCharacterLimits(t *testing.T) {
	configureHTTP(t)
	for _, test := range []struct {
		name, key string
		status    int
		calls     int
	}{
		{strings.Repeat("组", 128), strings.Repeat("路", 255), http.StatusOK, 1},
		{strings.Repeat("组", 129), "/key", http.StatusBadRequest, 0},
		{"group", strings.Repeat("路", 256), http.StatusBadRequest, 0},
		{"", "/key", http.StatusBadRequest, 0},
		{"group", "", http.StatusBadRequest, 0},
	} {
		stub := &handlerGroupRPCStub{}
		body, err := json.Marshal(map[string]string{"name": test.name, "discoveryKey": test.key})
		if err != nil {
			t.Fatal(err)
		}
		recorder, _ := invokeHandler(t, CreateExecutorGroupHandler(&svc.ServiceContext{JobRpc: stub}), "/job/group/create", string(body))
		if recorder.Code != test.status || stub.calls != test.calls {
			t.Fatalf("Unicode validation HTTP=%d calls=%d", recorder.Code, stub.calls)
		}
		if stub.calls == 1 && !proto.Equal(stub.request, &jobclient.CreateExecutorGroupRequest{Name: test.name, DiscoveryKey: test.key}) {
			t.Fatal("HTTP boundary modified group identity")
		}
	}
}

func TestGroupListValidatesEmbeddedPagination(t *testing.T) {
	configureHTTP(t)
	for _, body := range []string{`{}`, `{"page":0,"pageSize":20}`, `{"page":1,"pageSize":0}`, `{"page":1,"pageSize":201}`} {
		stub := &handlerGroupRPCStub{}
		recorder, result := invokeHandler(t, ListExecutorGroupsHandler(&svc.ServiceContext{JobRpc: stub}), "/job/group/list", body)
		if recorder.Code != http.StatusBadRequest || result.Subcode != subcode.InvalidRequest || stub.calls != 0 {
			t.Fatalf("invalid pagination reached RPC: HTTP=%d calls=%d", recorder.Code, stub.calls)
		}
	}
}
