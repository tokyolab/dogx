package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	commonresponse "github.com/tokyolab/dogx/pkg/response"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func TestProfileRouteRejectsInvalidFields(t *testing.T) {
	httpx.SetValidator(requestvalidator.New())
	t.Cleanup(func() { httpx.SetValidator(nil) })
	for _, body := range []string{`{"nickname":`, `{}`, `{"nickname":"Alice","email":"invalid"}`, `{"nickname":"` + strings.Repeat("中", 65) + `"}`, `{"nickname":"Alice","phone":"` + strings.Repeat("1", 33) + `"}`} {
		order := []string{}
		rpc := &routeSystemRPCStub{order: &order}
		server := newRouteTestServer(t, validRouteSessionReader(&order), &routeEnforcerStub{order: &order}, rpc)
		recorder := httptest.NewRecorder()
		server.Serve(recorder, newSecurityRouteRequest(routeSecurityCase{method: http.MethodPost, path: "/auth/profile/update", body: body}, signedRouteToken(t, 42, "session-id", []int64{7})))
		if recorder.Code != http.StatusBadRequest || rpc.updateProfileRequest != nil {
			t.Fatalf("invalid input reached RPC: %d %s", recorder.Code, recorder.Body.String())
		}
	}
}

func TestProfileRoutesAreSelfService(t *testing.T) {
	for _, path := range []string{"/auth/profile", "/auth/profile/update"} {
		for _, valid := range []bool{true, false} {
			order := []string{}
			sessions := validRouteSessionReader(&order)
			if !valid {
				sessions.err = authn.ErrSessionNotFound
			}
			enforcer := &routeEnforcerStub{order: &order, allowed: false}
			rpc := &routeSystemRPCStub{order: &order}
			server := newRouteTestServer(t, sessions, enforcer, rpc)
			recorder := httptest.NewRecorder()
			request := newSecurityRouteRequest(routeSecurityCase{method: http.MethodPost, path: path, body: `{"id":999,"userId":999,"nickname":"Alice","email":"a@example.com","phone":"123","departmentId":999,"roleIds":[1],"status":0}`}, signedRouteToken(t, 42, "session-id", []int64{7}))
			server.Serve(recorder, request)
			if !valid {
				assertRouteResponseCode(t, recorder, http.StatusUnauthorized, http.StatusUnauthorized)
				if strings.Join(order, ",") != "session" {
					t.Fatalf("invalid session reached RPC: %v", order)
				}
				continue
			}
			assertRouteResponseCode(t, recorder, http.StatusOK, commonresponse.SuccessCode)
			if strings.Join(order, ",") != "session,rpc" || len(enforcer.requests) != 0 {
				t.Fatalf("profile requires management permissions: %v", order)
			}
			if path == "/auth/profile" {
				if rpc.profileRequest == nil || rpc.profileRequest.UserId != 42 {
					t.Fatal("untrusted identity")
				}
			} else if rpc.updateProfileRequest == nil || rpc.updateProfileRequest.UserId != 42 || rpc.updateProfileRequest.Nickname != "Alice" || rpc.updateProfileRequest.Email != "a@example.com" || rpc.updateProfileRequest.Phone != "123" {
				t.Fatalf("untrusted update: %+v", rpc.updateProfileRequest)
			}
		}
	}
}
