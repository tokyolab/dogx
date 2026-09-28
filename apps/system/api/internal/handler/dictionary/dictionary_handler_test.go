package dictionary

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	commonresponse "github.com/tokyolab/dogx/pkg/response"

	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDictionaryHandlers(t *testing.T) {
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	httpx.SetValidator(requestvalidator.New())
	t.Cleanup(func() { httpx.SetOkHandler(nil); httpx.SetErrorHandlerCtx(nil); httpx.SetValidator(nil) })
	tests := []struct {
		name, path, body string
		hasBody          bool
		handler          func(*svc.ServiceContext) http.HandlerFunc
	}{
		{"ListDictionaries", "/dictionary/list", `{}`, false, ListDictionariesHandler},
		{"GetDictionary", "/dictionary/get", `{"id":1}`, true, GetDictionaryHandler},
		{"CreateDictionary", "/dictionary/create", `{"name":"test","code":"source","remark":"test","status":1,"isPublic":false}`, true, CreateDictionaryHandler},
		{"UpdateDictionary", "/dictionary/update", `{"id":1,"name":"test","remark":"test","isPublic":false}`, true, UpdateDictionaryHandler},
		{"UpdateDictionaryStatus", "/dictionary/status/update", `{"id":1,"status":1}`, true, UpdateDictionaryStatusHandler},
		{"DeleteDictionary", "/dictionary/delete", `{"id":1}`, true, DeleteDictionaryHandler},
		{"ListDictionaryItems", "/dictionary/item/list", `{"dictionaryId":1}`, true, ListDictionaryItemsHandler},
		{"GetDictionaryItem", "/dictionary/item/get", `{"id":1}`, true, GetDictionaryItemHandler},
		{"CreateDictionaryItem", "/dictionary/item/create", `{"dictionaryId":1,"label":"test","value":"test","sort":1,"remark":"test","status":1}`, true, CreateDictionaryItemHandler},
		{"UpdateDictionaryItem", "/dictionary/item/update", `{"id":1,"label":"test","sort":1,"remark":"test"}`, true, UpdateDictionaryItemHandler},
		{"UpdateDictionaryItemStatus", "/dictionary/item/status/update", `{"id":1,"status":1}`, true, UpdateDictionaryItemStatusHandler},
		{"DeleteDictionaryItem", "/dictionary/item/delete", `{"id":1}`, true, DeleteDictionaryItemHandler},
		{"ClearDictionaryCache", "/dictionary/cache/clear", `{}`, false, ClearDictionaryCacheHandler},
		{"ReadDictionaries", "/dictionary/read", `{"codes":["source"]}`, true, ReadDictionariesHandler},
		{"ReadPublicDictionaries", "/dictionary/public/read", `{"codes":["source"]}`, true, ReadPublicDictionariesHandler},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"success", "RPC error", "malformed", "invalid"} {
				if !tc.hasBody && (mode == "malformed" || mode == "invalid") {
					continue
				}
				rpc := &dictionaryHandlerRPCStub{}
				body := tc.body
				want := http.StatusOK
				if mode == "RPC error" {
					rpc.err = status.Error(codes.Unavailable, "RPC unavailable")
					want = http.StatusServiceUnavailable
				}
				if mode == "malformed" {
					body = "{"
					want = http.StatusBadRequest
				}
				if mode == "invalid" {
					body = "{}"
					want = http.StatusBadRequest
				}
				req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				out := httptest.NewRecorder()
				tc.handler(&svc.ServiceContext{SystemRpc: rpc})(out, req)
				if out.Code != want {
					t.Fatalf("%s: got %d want %d: %s", mode, out.Code, want, out.Body.String())
				}
				if (mode == "malformed" || mode == "invalid") && rpc.called != "" {
					t.Fatal("invalid request reached RPC")
				}
			}
		})
	}
}
