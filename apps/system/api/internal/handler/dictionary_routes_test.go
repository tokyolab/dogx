package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	commonresponse "github.com/tokyolab/dogx/pkg/response"
)

func TestDictionaryReadSecurityBoundary(t *testing.T) {
	for _, public := range []bool{false, true} {
		for _, valid := range []bool{false, true} {
			order := []string{}
			sessions := validRouteSessionReader(&order)
			if !valid {
				sessions.err = authn.ErrSessionNotFound
			}
			enforcer := &routeEnforcerStub{order: &order, allowed: false}
			rpc := &routeSystemRPCStub{order: &order}
			server := newRouteTestServer(t, sessions, enforcer, rpc)
			path := "/dictionary/read"
			token := signedRouteToken(t, 42, "session-id", []int64{7})
			if public {
				path = "/dictionary/public/read"
				token = ""
			}
			recorder := httptest.NewRecorder()
			server.Serve(recorder, newSecurityRouteRequest(routeSecurityCase{method: http.MethodPost, path: path, body: `{"codes":["source","internal"],"publicOnly":false}`}, token))
			if !public && !valid {
				assertRouteResponseCode(t, recorder, http.StatusUnauthorized, http.StatusUnauthorized)
				continue
			}
			assertRouteResponseCode(t, recorder, http.StatusOK, commonresponse.SuccessCode)
			if len(enforcer.requests) != 0 || rpc.dictionaryRead == nil || rpc.dictionaryRead.PublicOnly != public {
				t.Fatalf("incorrect read authorization: %v %+v", order, rpc.dictionaryRead)
			}
			if public && strings.Join(order, ",") != "rpc" {
				t.Fatal("public route unexpectedly requires session", order)
			}
		}
	}
}
