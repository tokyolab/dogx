package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
)

type batchEnforcerStub struct {
	requests [][]interface{}
	results  []bool
	err      error
}

func (s *batchEnforcerStub) BatchEnforce(requests [][]interface{}) ([]bool, error) {
	s.requests = requests
	return s.results, s.err
}

func TestAuthorizationAllowsWhenAnyRoleAllows(t *testing.T) {
	setAuthorizationResponseHandler(t)
	enforcer := &batchEnforcerStub{results: []bool{false, true}}
	nextCalled := false
	middleware := NewAuthorizationMiddleware(enforcer).Handle(func(http.ResponseWriter, *http.Request) {
		nextCalled = true
	})

	request := authorizationRequest([]int64{2, 7})
	recorder := httptest.NewRecorder()
	middleware.ServeHTTP(recorder, request)

	if !nextCalled {
		t.Fatal("allowed roles did not reach the handler")
	}
	if len(enforcer.requests) != 2 ||
		enforcer.requests[0][0] != "r:2" || enforcer.requests[1][0] != "r:7" ||
		enforcer.requests[0][1] != "/role/api/update" || enforcer.requests[0][2] != http.MethodPost {
		t.Fatalf("unexpected batch authorization requests: %v", enforcer.requests)
	}
}

func TestAuthorizationDefaultsToForbidden(t *testing.T) {
	setAuthorizationResponseHandler(t)
	tests := []struct {
		name     string
		roleIDs  []int64
		enforcer BatchEnforcer
	}{
		{name: "no roles", roleIDs: nil, enforcer: &batchEnforcerStub{}},
		{name: "all denied", roleIDs: []int64{2, 7}, enforcer: &batchEnforcerStub{results: []bool{false, false}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nextCalled := false
			middleware := NewAuthorizationMiddleware(test.enforcer).Handle(func(http.ResponseWriter, *http.Request) {
				nextCalled = true
			})
			recorder := httptest.NewRecorder()
			middleware.ServeHTTP(recorder, authorizationRequest(test.roleIDs))
			if nextCalled {
				t.Fatal("denied request reached handler")
			}
			if recorder.Code != http.StatusForbidden {
				t.Fatalf("unexpected denial status: %d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestAuthorizationFailsClosedWhenEnforcerIsUnavailable(t *testing.T) {
	setAuthorizationResponseHandler(t)
	tests := []struct {
		name     string
		enforcer BatchEnforcer
	}{
		{name: "nil enforcer"},
		{name: "enforcer error", enforcer: &batchEnforcerStub{err: errors.New("policy unavailable")}},
		{name: "result mismatch", enforcer: &batchEnforcerStub{results: []bool{true}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			NewAuthorizationMiddleware(test.enforcer).Handle(func(http.ResponseWriter, *http.Request) {
				t.Fatal("unavailable authorization reached handler")
			}).ServeHTTP(recorder, authorizationRequest([]int64{2, 7}))
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("unexpected unavailable status: %d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func authorizationRequest(roleIDs []int64) *http.Request {
	ctx := context.WithValue(context.Background(), "userId", int64(42))
	ctx = context.WithValue(ctx, "sessionId", "session-id")
	ctx = context.WithValue(ctx, "roleIds", roleIDs)
	return httptest.NewRequest(http.MethodPost, "/role/api/update", nil).WithContext(ctx)
}

func setAuthorizationResponseHandler(t *testing.T) {
	t.Helper()
	httpx.SetErrorHandlerCtx(response.HandleError)
	t.Cleanup(func() { httpx.SetErrorHandlerCtx(nil) })
}
