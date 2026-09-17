package menu

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	systemsubcode "github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	commonresponse "github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const menuFieldsJSON = `"parentId":7,"type":2,"name":"页面","routeName":"Child","path":"/child","component":"system/child/index","sort":0,"visible":false,"keepAlive":false,"external":false`
const createMenuJSON = `{` + menuFieldsJSON + `,"status":0}`
const updateMenuJSON = `{` + menuFieldsJSON + `,"id":9}`

type menuHandlerRPCStub struct {
	systemclient.System
	called  string
	request proto.Message
	err     error
}

func (s *menuHandlerRPCStub) CreateMenu(_ context.Context, req *systemclient.CreateMenuRequest, _ ...grpc.CallOption) (*systemclient.CreateMenuResponse, error) {
	s.called, s.request = "CreateMenu", req
	return &systemclient.CreateMenuResponse{Id: 9}, s.err
}

func (s *menuHandlerRPCStub) UpdateMenu(_ context.Context, req *systemclient.UpdateMenuRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called, s.request = "UpdateMenu", req
	return &systemclient.EmptyResponse{}, s.err
}

func (s *menuHandlerRPCStub) UpdateMenuStatus(_ context.Context, req *systemclient.UpdateMenuStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called, s.request = "UpdateMenuStatus", req
	return &systemclient.EmptyResponse{}, s.err
}

func (s *menuHandlerRPCStub) DeleteMenu(_ context.Context, req *systemclient.DeleteMenuRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called, s.request = "DeleteMenu", req
	return &systemclient.EmptyResponse{}, s.err
}

func (s *menuHandlerRPCStub) GetMenu(_ context.Context, req *systemclient.GetMenuRequest, _ ...grpc.CallOption) (*systemclient.GetMenuResponse, error) {
	s.called, s.request = "GetMenu", req
	return &systemclient.GetMenuResponse{Menu: &systemclient.MenuInfo{
		Id: 9, Menu: &systemclient.MenuFields{ParentId: 7, Type: 2, Name: "页面"},
	}}, s.err
}

func (s *menuHandlerRPCStub) ListMenus(_ context.Context, req *systemclient.ListMenusRequest, _ ...grpc.CallOption) (*systemclient.ListMenusResponse, error) {
	s.called, s.request = "ListMenus", req
	return &systemclient.ListMenusResponse{}, s.err
}

func setupMenuResponseHandlers(t *testing.T) {
	t.Helper()
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	httpx.SetValidator(requestvalidator.New())
	t.Cleanup(func() {
		httpx.SetOkHandler(nil)
		httpx.SetErrorHandlerCtx(nil)
		httpx.SetValidator(nil)
	})
}

func serveMenuRequest(handler func(*svc.ServiceContext) http.HandlerFunc, rpc *menuHandlerRPCStub, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/menu/test", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler(&svc.ServiceContext{SystemRpc: rpc})(recorder, req)
	return recorder
}

func assertMenuHTTPResponse(t *testing.T, recorder *httptest.ResponseRecorder, wantStatus int, wantCode uint32, wantSubcode string) json.RawMessage {
	t.Helper()
	var envelope struct {
		Code    uint32          `json:"code"`
		Subcode string          `json:"subcode"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
	}
	if recorder.Code != wantStatus || envelope.Code != wantCode || envelope.Subcode != wantSubcode || envelope.Message == "" || len(envelope.Data) == 0 {
		t.Fatalf("unexpected response contract: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if wantCode != commonresponse.SuccessCode && string(envelope.Data) != "null" {
		t.Fatalf("error response contains data: %s", envelope.Data)
	}
	return envelope.Data
}

func TestMenuHTTPSuccessPreservesRequestsAndResponseShape(t *testing.T) {
	setupMenuResponseHandlers(t)
	fields := &systemclient.MenuFields{
		ParentId: 7, Type: 2, Name: "页面", RouteName: "Child", Path: "/child",
		Component: "system/child/index", Sort: 0, Visible: false, KeepAlive: false, External: false,
	}
	for _, tc := range []struct {
		method      string
		body        string
		handler     func(*svc.ServiceContext) http.HandlerFunc
		wantRequest proto.Message
		wantData    string
	}{
		{"CreateMenu", createMenuJSON, CreateMenuHandler, &systemclient.CreateMenuRequest{Menu: fields, Status: 0}, `{"id":9}`},
		{"UpdateMenu", updateMenuJSON, UpdateMenuHandler, &systemclient.UpdateMenuRequest{Id: 9, Menu: fields}, `{}`},
		{"UpdateMenuStatus", `{"id":9,"status":0}`, UpdateMenuStatusHandler, &systemclient.UpdateMenuStatusRequest{Id: 9, Status: 0}, `{}`},
		{"DeleteMenu", `{"id":9}`, DeleteMenuHandler, &systemclient.DeleteMenuRequest{Id: 9}, `{}`},
		{"ListMenus", "", ListMenusHandler, &systemclient.ListMenusRequest{}, `{"items":[]}`},
		{"GetMenu", `{"id":9}`, GetMenuHandler, &systemclient.GetMenuRequest{Id: 9},
			`{"id":9,"parentId":7,"type":2,"name":"页面","routeName":"","path":"","component":"","permission":"","icon":"","sort":0,"visible":false,"keepAlive":false,"external":false,"remark":"","status":0,"createdAt":"","updatedAt":""}`},
	} {
		t.Run(tc.method, func(t *testing.T) {
			rpc := &menuHandlerRPCStub{}
			recorder := serveMenuRequest(tc.handler, rpc, tc.body)
			data := assertMenuHTTPResponse(t, recorder, http.StatusOK, commonresponse.SuccessCode, "")
			if rpc.called != tc.method || !proto.Equal(rpc.request, tc.wantRequest) {
				t.Fatalf("RPC request changed: method=%s request=%v want=%v", rpc.called, rpc.request, tc.wantRequest)
			}
			var got, want any
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(tc.wantData), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("response data=%s want=%s", data, tc.wantData)
			}
		})
	}
}

func TestMenuHTTPRejectsInvalidFieldsBeforeRPC(t *testing.T) {
	setupMenuResponseHandlers(t)
	for _, tc := range []struct {
		name    string
		body    string
		handler func(*svc.ServiceContext) http.HandlerFunc
	}{
		{"malformed JSON", `{"parentId":`, CreateMenuHandler},
		{"wrong field type", strings.Replace(createMenuJSON, `"sort":0`, `"sort":"invalid"`, 1), CreateMenuHandler},
		{"invalid create status", strings.Replace(createMenuJSON, `"status":0`, `"status":2`, 1), CreateMenuHandler},
		{"invalid menu type", strings.Replace(createMenuJSON, `"type":2`, `"type":4`, 1), CreateMenuHandler},
		{"negative parent", strings.Replace(updateMenuJSON, `"parentId":7`, `"parentId":-1`, 1), UpdateMenuHandler},
		{"empty name", strings.Replace(updateMenuJSON, "页面", "", 1), UpdateMenuHandler},
		{"long Chinese name", strings.Replace(updateMenuJSON, "页面", strings.Repeat("中", 65), 1), UpdateMenuHandler},
		{"invalid update id", strings.Replace(updateMenuJSON, `"id":9`, `"id":0`, 1), UpdateMenuHandler},
		{"invalid get id", `{"id":0}`, GetMenuHandler},
		{"invalid delete id", `{"id":-1}`, DeleteMenuHandler},
		{"invalid status update id", `{"id":0,"status":0}`, UpdateMenuStatusHandler},
		{"truncating status", `{"id":9,"status":65536}`, UpdateMenuStatusHandler},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpc := &menuHandlerRPCStub{}
			recorder := serveMenuRequest(tc.handler, rpc, tc.body)
			assertMenuHTTPResponse(t, recorder, http.StatusBadRequest, http.StatusBadRequest, "common.invalid_request")
			if rpc.called != "" {
				t.Fatalf("invalid request reached RPC: %s", rpc.called)
			}
		})
	}
}

func TestMenuHTTPErrorContracts(t *testing.T) {
	setupMenuResponseHandlers(t)
	const diagnostic = "private-database-or-rpc-diagnostic"
	businessError := func(reason string) error {
		t.Helper()
		withDetails, err := status.New(codes.Code(bizerror.DefaultCode), diagnostic).
			WithDetails(&errdetails.ErrorInfo{Reason: reason})
		if err != nil {
			t.Fatal(err)
		}
		return withDetails.Err()
	}
	for _, tc := range []struct {
		name       string
		method     string
		body       string
		handler    func(*svc.ServiceContext) http.HandlerFunc
		rpcErr     error
		wantStatus int
		wantCode   uint32
		wantReason string
	}{
		{"create route name conflict", "CreateMenu", createMenuJSON, CreateMenuHandler, businessError(systemsubcode.MenuRouteNameExists), http.StatusOK, bizerror.DefaultCode, systemsubcode.MenuRouteNameExists},
		{"update path conflict", "UpdateMenu", updateMenuJSON, UpdateMenuHandler, businessError(systemsubcode.MenuPathExists), http.StatusOK, bizerror.DefaultCode, systemsubcode.MenuPathExists},
		{"get missing menu", "GetMenu", `{"id":9}`, GetMenuHandler, businessError(systemsubcode.MenuNotFound), http.StatusOK, bizerror.DefaultCode, systemsubcode.MenuNotFound},
		{"delete parent", "DeleteMenu", `{"id":9}`, DeleteMenuHandler, businessError(systemsubcode.MenuHasChildren), http.StatusOK, bizerror.DefaultCode, systemsubcode.MenuHasChildren},
		{"status missing menu", "UpdateMenuStatus", `{"id":9,"status":0}`, UpdateMenuStatusHandler, businessError(systemsubcode.MenuNotFound), http.StatusOK, bizerror.DefaultCode, systemsubcode.MenuNotFound},
		{"list RPC unavailable", "ListMenus", "", ListMenusHandler, status.Error(codes.Unavailable, diagnostic), http.StatusServiceUnavailable, http.StatusServiceUnavailable, "common.service_unavailable"},
		{"update internal failure", "UpdateMenu", updateMenuJSON, UpdateMenuHandler, status.Error(codes.Internal, diagnostic), http.StatusInternalServerError, http.StatusInternalServerError, "common.internal_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpc := &menuHandlerRPCStub{err: tc.rpcErr}
			recorder := serveMenuRequest(tc.handler, rpc, tc.body)
			assertMenuHTTPResponse(t, recorder, tc.wantStatus, tc.wantCode, tc.wantReason)
			if rpc.called != tc.method {
				t.Fatalf("called RPC=%s want=%s", rpc.called, tc.method)
			}
			if strings.Contains(recorder.Body.String(), diagnostic) {
				t.Fatal("internal diagnostic leaked into HTTP response")
			}
		})
	}
}

var _ systemclient.System = (*menuHandlerRPCStub)(nil)
