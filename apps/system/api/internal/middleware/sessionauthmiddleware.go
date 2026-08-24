// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package middleware

import (
	"errors"
	"net/http"
	"time"

	"github.com/tokyolab/dogx/apps/system/api/internal/authctx"
	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/pkg/response"

	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SessionAuthMiddleware struct {
	sessions authn.SessionReader
}

func NewSessionAuthMiddleware(sessions authn.SessionReader) *SessionAuthMiddleware {
	return &SessionAuthMiddleware{sessions: sessions}
}

func (m *SessionAuthMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		identity, err := authctx.FromContext(r.Context())
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unauthenticated, "authentication required"))
			return
		}
		if m.sessions == nil {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unavailable, "authentication service unavailable"))
			return
		}

		session, err := m.sessions.Get(r.Context(), identity.SessionID)
		if errors.Is(err, authn.ErrSessionNotFound) {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unauthenticated, "session is invalid or expired"))
			return
		}
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, response.ServiceUnavailable(err))
			return
		}
		if session.UserID != identity.UserID || !session.ExpiresAt.After(time.Now().UTC()) {
			httpx.ErrorCtx(r.Context(), w, status.Error(codes.Unauthenticated, "session is invalid or expired"))
			return
		}

		next(w, r)
	}
}
