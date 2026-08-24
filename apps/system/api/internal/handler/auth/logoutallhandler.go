// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package auth

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/auth"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// Revoke all sessions of the current user
func LogoutAllHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := auth.NewLogoutAllLogic(r.Context(), svcCtx)
		resp, err := l.LogoutAll()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
