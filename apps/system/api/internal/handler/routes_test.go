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
	order              *[]string
	called             string
	request            *systemclient.ReplaceRoleAPIsRequest
	listRolesRequest   *systemclient.ListRolesRequest
	getRoleRequest     *systemclient.GetRoleRequest
	listAPIsRequest    *systemclient.ListAPIsRequest
	getRoleAPIsRequest *systemclient.GetRoleAPIsRequest
}

func (s *routeSystemRPCStub) CreateRole(
	_ context.Context,
	_ *systemclient.CreateRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.CreateRoleResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "CreateRole"
	return &systemclient.CreateRoleResponse{Id: 13}, nil
}

func (s *routeSystemRPCStub) UpdateRole(
	_ context.Context,
	_ *systemclient.UpdateRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateRole"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) UpdateRoleStatus(
	_ context.Context,
	_ *systemclient.UpdateRoleStatusRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "UpdateRoleStatus"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) DeleteRole(
	_ context.Context,
	_ *systemclient.DeleteRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "DeleteRole"
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) ReplaceRoleAPIs(
	_ context.Context,
	request *systemclient.ReplaceRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.EmptyResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ReplaceRoleAPIs"
	s.request = request
	return &systemclient.EmptyResponse{}, nil
}

func (s *routeSystemRPCStub) ListRoles(
	_ context.Context,
	request *systemclient.ListRolesRequest,
	_ ...grpc.CallOption,
) (*systemclient.ListRolesResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ListRoles"
	s.listRolesRequest = request
	return &systemclient.ListRolesResponse{
		Items: []*systemclient.RoleInfo{{Id: 7, Code: "operator", Name: "Operator"}},
		Total: 1,
	}, nil
}

func (s *routeSystemRPCStub) GetRole(
	_ context.Context,
	request *systemclient.GetRoleRequest,
	_ ...grpc.CallOption,
) (*systemclient.GetRoleResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "GetRole"
	s.getRoleRequest = request
	return &systemclient.GetRoleResponse{
		Role: &systemclient.RoleInfo{Id: request.Id, Code: "operator", Name: "Operator"},
	}, nil
}

func (s *routeSystemRPCStub) ListAPIs(
	_ context.Context,
	request *systemclient.ListAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.ListAPIsResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "ListAPIs"
	s.listAPIsRequest = request
	return &systemclient.ListAPIsResponse{
		Items: []*systemclient.APIInfo{{
			Id:          11,
			ServiceName: "system-api",
			ApiGroup:    "角色管理",
			Name:        "查询角色",
			Path:        "/role/get",
			Method:      http.MethodPost,
			Status:      1,
		}},
	}, nil
}

func (s *routeSystemRPCStub) GetRoleAPIs(
	_ context.Context,
	request *systemclient.GetRoleAPIsRequest,
	_ ...grpc.CallOption,
) (*systemclient.GetRoleAPIsResponse, error) {
	*s.order = append(*s.order, "rpc")
	s.called = "GetRoleAPIs"
	s.getRoleAPIsRequest = request
	return &systemclient.GetRoleAPIsResponse{ApiIds: []int64{11, 12}}, nil
}

type routeSecurityLevel int

const (
	routePublic routeSecurityLevel = iota
	routeAuthenticated
	routeAuthorized
)

type routeSecurityCase struct {
	name         string
	method       string
	path         string
	body         string
	level        routeSecurityLevel
	publicStatus int
	rpcMethod    string
}

var routeSecurityMatrix = []routeSecurityCase{
	{name: "login", method: http.MethodPost, path: "/auth/login", body: `{"username":`, level: routePublic, publicStatus: http.StatusBadRequest},
	{name: "refresh", method: http.MethodPost, path: "/auth/refresh", body: `{"refreshToken":`, level: routePublic, publicStatus: http.StatusBadRequest},
	{name: "health", method: http.MethodGet, path: "/health", level: routePublic, publicStatus: http.StatusOK},
	{name: "ready", method: http.MethodGet, path: "/ready", level: routePublic, publicStatus: http.StatusServiceUnavailable},
	{name: "current user", method: http.MethodPost, path: "/auth/me", level: routeAuthenticated},
	{name: "logout", method: http.MethodPost, path: "/auth/logout", level: routeAuthenticated},
	{name: "logout all", method: http.MethodPost, path: "/auth/logout-all", level: routeAuthenticated},
	{name: "change password", method: http.MethodPost, path: "/auth/change-password", body: `{}`, level: routeAuthenticated},
	{name: "API list", method: http.MethodPost, path: "/api/list", body: `{}`, level: routeAuthorized, rpcMethod: "ListAPIs"},
	{name: "role create", method: http.MethodPost, path: "/role/create", body: `{"code":"operator","name":"Operator","sort":10,"status":1}`, level: routeAuthorized, rpcMethod: "CreateRole"},
	{name: "role list", method: http.MethodPost, path: "/role/list", body: `{"page":1,"pageSize":20}`, level: routeAuthorized, rpcMethod: "ListRoles"},
	{name: "role get", method: http.MethodPost, path: "/role/get", body: `{"id":9}`, level: routeAuthorized, rpcMethod: "GetRole"},
	{name: "role update", method: http.MethodPost, path: "/role/update", body: `{"id":9,"code":"operator","name":"Operator","sort":10}`, level: routeAuthorized, rpcMethod: "UpdateRole"},
	{name: "role status update", method: http.MethodPost, path: "/role/status/update", body: `{"id":9,"status":0}`, level: routeAuthorized, rpcMethod: "UpdateRoleStatus"},
	{name: "role delete", method: http.MethodPost, path: "/role/delete", body: `{"id":9}`, level: routeAuthorized, rpcMethod: "DeleteRole"},
	{name: "role API get", method: http.MethodPost, path: "/role/api/get", body: `{"roleId":9}`, level: routeAuthorized, rpcMethod: "GetRoleAPIs"},
	{name: "role API update", method: http.MethodPost, path: "/role/api/update", body: `{"roleId":9,"apiIds":[11,12]}`, level: routeAuthorized, rpcMethod: "ReplaceRoleAPIs"},
}

func TestRegisteredRoutesRespectSecurityMatrix(t *testing.T) {
	for _, test := range routeSecurityMatrix {
		t.Run(test.name, func(t *testing.T) {
			order := []string{}
			rpc := &routeSystemRPCStub{order: &order}
			server := newRouteTestServer(
				t,
				validRouteSessionReader(&order),
				&routeEnforcerStub{order: &order},
				rpc,
			)
			recorder := httptest.NewRecorder()
			server.Serve(recorder, newSecurityRouteRequest(test, ""))

			if test.level == routePublic {
				if recorder.Code != test.publicStatus {
					t.Fatalf(
						"public route status = %d, want %d body=%s",
						recorder.Code,
						test.publicStatus,
						recorder.Body.String(),
					)
				}
			} else {
				assertRouteResponseCode(t, recorder, http.StatusUnauthorized, http.StatusUnauthorized)
			}
			if len(order) != 0 || rpc.request != nil {
				t.Fatalf(
					"unauthenticated route reached protected dependencies: order=%v request=%+v",
					order,
					rpc.request,
				)
			}
		})
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

func TestAuthorizedRoutesRejectDeniedRoleBeforeRPC(t *testing.T) {
	for _, test := range routeSecurityMatrix {
		if test.level != routeAuthorized {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			order := []string{}
			enforcer := &routeEnforcerStub{order: &order, allowed: false}
			rpc := &routeSystemRPCStub{order: &order}
			server := newRouteTestServer(t, validRouteSessionReader(&order), enforcer, rpc)
			recorder := httptest.NewRecorder()
			server.Serve(
				recorder,
				newSecurityRouteRequest(
					test,
					signedRouteToken(t, 42, "session-id", []int64{7}),
				),
			)

			assertRouteResponseCode(t, recorder, http.StatusForbidden, http.StatusForbidden)
			if strings.Join(order, ",") != "session,authorization" || rpc.request != nil {
				t.Fatalf("denied role reached RPC: order=%v request=%+v", order, rpc.request)
			}
			if len(enforcer.requests) != 1 || enforcer.requests[0][0] != "r:7" ||
				enforcer.requests[0][1] != test.path ||
				enforcer.requests[0][2] != test.method {
				t.Fatalf("unexpected authorization request: %v", enforcer.requests)
			}
		})
	}
}

func TestAuthorizedRoutesAllowAuthorizedRole(t *testing.T) {
	for _, test := range routeSecurityMatrix {
		if test.level != routeAuthorized {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			order := []string{}
			rpc := &routeSystemRPCStub{order: &order}
			server := newRouteTestServer(
				t,
				validRouteSessionReader(&order),
				&routeEnforcerStub{order: &order, allowed: true},
				rpc,
			)
			recorder := httptest.NewRecorder()
			server.Serve(
				recorder,
				newSecurityRouteRequest(
					test,
					signedRouteToken(t, 42, "session-id", []int64{7}),
				),
			)

			assertRouteResponseCode(t, recorder, http.StatusOK, commonresponse.SuccessCode)
			if strings.Join(order, ",") != "session,authorization,rpc" {
				t.Fatalf("unexpected protected route execution order: %v", order)
			}
			if rpc.called != test.rpcMethod {
				t.Fatalf("called RPC = %q, want %q", rpc.called, test.rpcMethod)
			}
			if test.rpcMethod == "ReplaceRoleAPIs" &&
				(rpc.request == nil || rpc.request.RoleId != 9 ||
					len(rpc.request.ApiIds) != 2 ||
					rpc.request.ApiIds[0] != 11 ||
					rpc.request.ApiIds[1] != 12) {
				t.Fatalf("unexpected role policy RPC request: %+v", rpc.request)
			}
		})
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

func newSecurityRouteRequest(test routeSecurityCase, token string) *http.Request {
	request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
	if test.body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	return request
}

func signedRouteToken(t testing.TB, userID int64, sessionID string, roleIDs []int64) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"userId":       userID,
		"sessionId":    sessionID,
		"roleIds":      roleIDs,
		"isSuperAdmin": false,
		"exp":          time.Now().UTC().Add(time.Hour).Unix(),
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
