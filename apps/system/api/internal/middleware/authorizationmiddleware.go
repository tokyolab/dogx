// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package middleware

import (
	"errors"
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/authctx"
	"github.com/tokyolab/dogx/apps/system/internal/authorization"
	"github.com/tokyolab/dogx/pkg/response"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BatchEnforcer interface {
	BatchEnforce(requests [][]interface{}) ([]bool, error)
}

type AuthorizationMiddleware struct {
	enforcer BatchEnforcer
}

func NewAuthorizationMiddleware(enforcer BatchEnforcer) *AuthorizationMiddleware {
	return &AuthorizationMiddleware{enforcer: enforcer}
}

func (m *AuthorizationMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		identity, err := authctx.FromContext(r.Context())
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unauthenticated, "authentication required"))
			return
		}
		if len(identity.RoleIDs) == 0 {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.PermissionDenied, "permission denied"))
			return
		}
		if m.enforcer == nil {
			httpx.ErrorCtx(r.Context(), w, response.ServiceUnavailable(errors.New("authorization service unavailable")))
			return
		}

		requests := make([][]interface{}, 0, len(identity.RoleIDs))
		for _, roleID := range identity.RoleIDs {
			subject, err := authorization.RoleSubject(roleID)
			if err != nil {
				httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unauthenticated, "authentication required"))
				return
			}
			requests = append(requests, []interface{}{subject, r.URL.Path, r.Method})
		}

		allowed, err := m.enforcer.BatchEnforce(requests)
		if err != nil || len(allowed) != len(requests) {
			if err == nil {
				err = errors.New("authorization result count mismatch")
			}
			httpx.ErrorCtx(r.Context(), w, response.ServiceUnavailable(err))
			return
		}
		for _, result := range allowed {
			if result {
				next(w, r)
				return
			}
		}

		httpx.ErrorCtx(r.Context(), w, status.Error(codes.PermissionDenied, "permission denied"))
	}
}
