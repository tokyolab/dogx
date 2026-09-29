package loginlog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	"github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type loginLogHandlerRPCStub struct {
	systemclient.System
	request *systemclient.ListLoginLogsRequest
	err     error
}

func (s *loginLogHandlerRPCStub) ListLoginLogs(_ context.Context, req *systemclient.ListLoginLogsRequest, _ ...grpc.CallOption) (*systemclient.ListLoginLogsResponse, error) {
	s.request = req
	return &systemclient.ListLoginLogsResponse{Items: []*systemclient.LoginLogInfo{}, Total: 0}, s.err
}

func TestListLoginLogsHTTP(t *testing.T) {
	httpx.SetValidator(requestvalidator.New())
	httpx.SetOkHandler(response.HandleSuccess)
	httpx.SetErrorHandlerCtx(response.HandleError)
	t.Cleanup(func() { httpx.SetValidator(nil); httpx.SetOkHandler(nil); httpx.SetErrorHandlerCtx(nil) })
	for _, tc := range []struct {
		name, body string
		want       int
		called     bool
		rpcErr     error
	}{
		{"all", `{"page":1,"pageSize":20}`, 200, true, nil},
		{"failure", `{"page":1,"pageSize":20,"result":"failure"}`, 200, true, nil},
		{"success", `{"page":1,"pageSize":20,"result":"success"}`, 200, true, nil},
		{"malformed", `{`, 400, false, nil},
		{"missing page", `{"pageSize":20}`, 400, false, nil},
		{"large page size", `{"page":1,"pageSize":201}`, 400, false, nil},
		{"invalid filter", `{"page":1,"pageSize":20,"result":"false"}`, 400, false, nil},
		{"long username", `{"page":1,"pageSize":20,"username":"` + strings.Repeat("a", 65) + `"}`, 400, false, nil},
		{"rpc failure", `{"page":1,"pageSize":20}`, 503, true, status.Error(codes.Unavailable, "private database address")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpc := &loginLogHandlerRPCStub{err: tc.rpcErr}
			req := httptest.NewRequest(http.MethodPost, "/login-log/list", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			ListLoginLogsHandler(&svc.ServiceContext{SystemRpc: rpc})(recorder, req)
			if recorder.Code != tc.want || (rpc.request != nil) != tc.called {
				t.Fatalf("response: %d %s, request: %+v", recorder.Code, recorder.Body, rpc.request)
			}
			var envelope struct {
				Code    int                    `json:"code"`
				Subcode string                 `json:"subcode"`
				Data    types.LoginLogListResp `json:"data"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if tc.want == 200 {
				if envelope.Code != 0 || envelope.Data.Items == nil || len(envelope.Data.Items) != 0 || envelope.Data.Total != 0 {
					t.Fatalf("envelope: %+v", envelope)
				}
				if tc.name == "failure" && rpc.request.Result != "failure" {
					t.Fatal("failure filter lost")
				}
			} else if envelope.Code == 0 || envelope.Subcode == "" {
				t.Fatalf("error envelope: %+v", envelope)
			}
			if strings.Contains(recorder.Body.String(), "private database address") {
				t.Fatal("internal error leaked")
			}
		})
	}
}
