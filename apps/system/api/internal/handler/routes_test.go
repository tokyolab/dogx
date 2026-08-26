package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/middleware"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	commonresponse "github.com/tokyolab/dogx/pkg/response"

	"github.com/golang-jwt/jwt/v4"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
)

const routeTestAccessSecret = "0123456789abcdef0123456789abcdef"

type routeSessionReaderStub struct {
	order   *[]string
	session *authn.Session
	err     error
}

func (s *routeSessionReaderStub) Get(_ context.Context, _ string) (*authn.Session, error) {
	*s.order = append(*s.order, "session")
	return s.session, s.err
}

type routeEnforcerStub struct {
	order    *[]string
	allowed  bool
	requests [][]interface{}
}

func (s *routeEnforcerStub) BatchEnforce(requests [][]interface{}) ([]bool, error) {
	*s.order = append(*s.order, "authorization")
	s.requests = requests
	results := make([]bool, len(requests))
	for index := range results {
		results[index] = s.allowed
	}
	return results, nil
}

type routeSystemRPCStub struct {
	systemclient.System
	order   *[]string
	request *systemclient.ReplaceRoleAPIsRequest
}

func (s *routeSystemRPCStub) ReplaceRoleAPIs(
	_ context.Context,
	request *systemclient.ReplaceRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.request = request
	return &systemclient.EmptyResponse{}, nil
}

func TestRegisteredRoleRouteRequiresJWTBeforeSessionLookup(t *testing.T) {
	order := []string{}
	sessions := &routeSessionReaderStub{order: &order}
	enforcer := &routeEnforcerStub{order: &order}
	rpc := &routeSystemRPCStub{order: &order}
	server := newRouteTestServer(t, sessions, enforcer, rpc)

	request := newRoleRouteRequest(t, "")
	recorder := httptest.NewRecorder()
	server.Serve(recorder, request)

	assertRouteResponseCode(t, recorder, http.StatusUnauthorized, http.StatusUnauthorized)
	if len(order) != 0 || rpc.request != nil {
		t.Fatalf("request without JWT reached protected dependencies: order=%v request=%+v", order, rpc.request)
	}
}

func TestRegisteredRoleRouteChecksSessionBeforeAuthorization(t *testing.T) {
	order := []string{}
	sessions := &routeSessionReaderStub{order: &order, err: authn.ErrSessionNotFound}
	enforcer := &routeEnforcerStub{order: &order, allowed: true}
	rpc := &routeSystemRPCStub{order: &order}
	server := newRouteTestServer(t, sessions, enforcer, rpc)

	request := newRoleRouteRequest(t, signedRouteToken(t, 42, "session-id", []int64{7}))
	recorder := httptest.NewRecorder()
	server.Serve(recorder, request)

	assertRouteResponseCode(t, recorder, http.StatusUnauthorized, http.StatusUnauthorized)
	if len(order) != 1 || order[0] != "session" || rpc.request != nil {
		t.Fatalf("invalid session reached authorization or RPC: order=%v request=%+v", order, rpc.request)
	}
}

func TestRegisteredRoleRouteRejectsUnauthorizedRoleBeforeRPC(t *testing.T) {
	order := []string{}
	sessions := validRouteSessionReader(&order)
	enforcer := &routeEnforcerStub{order: &order, allowed: false}
	rpc := &routeSystemRPCStub{order: &order}
	server := newRouteTestServer(t, sessions, enforcer, rpc)

	request := newRoleRouteRequest(t, signedRouteToken(t, 42, "session-id", []int64{7}))
	recorder := httptest.NewRecorder()
	server.Serve(recorder, request)

	assertRouteResponseCode(t, recorder, http.StatusForbidden, http.StatusForbidden)
	if strings.Join(order, ",") != "session,authorization" || rpc.request != nil {
		t.Fatalf("denied role reached RPC: order=%v request=%+v", order, rpc.request)
	}
	if len(enforcer.requests) != 1 || enforcer.requests[0][0] != "r:7" ||
		enforcer.requests[0][1] != "/role/api/update" || enforcer.requests[0][2] != http.MethodPost {
		t.Fatalf("unexpected authorization request: %v", enforcer.requests)
	}
}

func TestRegisteredRoleRouteAllowsAuthorizedRole(t *testing.T) {
	order := []string{}
	sessions := validRouteSessionReader(&order)
	enforcer := &routeEnforcerStub{order: &order, allowed: true}
	rpc := &routeSystemRPCStub{order: &order}
	server := newRouteTestServer(t, sessions, enforcer, rpc)

	request := newRoleRouteRequest(t, signedRouteToken(t, 42, "session-id", []int64{7}))
	recorder := httptest.NewRecorder()
	server.Serve(recorder, request)

	assertRouteResponseCode(t, recorder, http.StatusOK, commonresponse.SuccessCode)
	if strings.Join(order, ",") != "session,authorization,rpc" {
		t.Fatalf("unexpected protected route execution order: %v", order)
	}
	if rpc.request == nil || rpc.request.RoleId != 9 || len(rpc.request.ApiIds) != 2 ||
		rpc.request.ApiIds[0] != 11 || rpc.request.ApiIds[1] != 12 {
		t.Fatalf("unexpected role policy RPC request: %+v", rpc.request)
	}
}

func TestRegisteredRoleRouteRejectsWrongHTTPMethod(t *testing.T) {
	order := []string{}
	server := newRouteTestServer(
		t,
		validRouteSessionReader(&order),
		&routeEnforcerStub{order: &order, allowed: true},
		&routeSystemRPCStub{order: &order},
	)
	request := httptest.NewRequest(http.MethodGet, "/role/api/update", nil)
	recorder := httptest.NewRecorder()

	server.Serve(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong HTTP method status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
	if len(order) != 0 {
		t.Fatalf("wrong HTTP method reached protected route dependencies: %v", order)
	}
}

func newRouteTestServer(
	t *testing.T,
	sessions authn.SessionReader,
	enforcer middleware.BatchEnforcer,
	rpc systemclient.System,
) *rest.Serverless {
	t.Helper()
	httpx.SetOkHandler(commonresponse.HandleSuccess)
	httpx.SetErrorHandlerCtx(commonresponse.HandleError)
	t.Cleanup(func() {
		httpx.SetOkHandler(nil)
		httpx.SetErrorHandlerCtx(nil)
	})

	server, err := rest.NewServer(
		rest.RestConf{},
		rest.WithUnauthorizedCallback(commonresponse.HandleUnauthorized),
	)
	if err != nil {
		t.Fatalf("create route test server: %v", err)
	}
	serviceCtx := &svc.ServiceContext{
		Config:        config.Config{Auth: config.AuthConf{AccessSecret: routeTestAccessSecret}},
		SystemRpc:     rpc,
		Sessions:      sessions,
		SessionAuth:   middleware.NewSessionAuthMiddleware(sessions).Handle,
		Authorization: middleware.NewAuthorizationMiddleware(enforcer).Handle,
	}
	RegisterHandlers(server, serviceCtx)
	serverless, err := rest.NewServerless(server)
	if err != nil {
		t.Fatalf("build registered route server: %v", err)
	}
	return serverless
}

func validRouteSessionReader(order *[]string) *routeSessionReaderStub {
	return &routeSessionReaderStub{
		order: order,
		session: &authn.Session{
			ID:        "session-id",
			UserID:    42,
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
	}
}

func newRoleRouteRequest(t testing.TB, token string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPost,
		"/role/api/update",
		strings.NewReader(`{"roleId":9,"apiIds":[11,12]}`),
	)
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	return request
}

func signedRouteToken(t testing.TB, userID int64, sessionID string, roleIDs []int64) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"userId":    userID,
		"sessionId": sessionID,
		"roleIds":   roleIDs,
		"exp":       time.Now().UTC().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte(routeTestAccessSecret))
	if err != nil {
		t.Fatalf("sign route test JWT: %v", err)
	}
	return signed
}

func assertRouteResponseCode(t testing.TB, recorder *httptest.ResponseRecorder, statusCode int, code uint32) {
	t.Helper()
	if recorder.Code != statusCode {
		t.Fatalf("unexpected HTTP status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	var body commonresponse.Body
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode route response: %v", err)
	}
	if body.Code != code {
		t.Fatalf("response code = %d, want %d body=%s", body.Code, code, recorder.Body.String())
	}
}

var _ authn.SessionReader = (*routeSessionReaderStub)(nil)
var _ middleware.BatchEnforcer = (*routeEnforcerStub)(nil)
var _ systemclient.System = (*routeSystemRPCStub)(nil)
