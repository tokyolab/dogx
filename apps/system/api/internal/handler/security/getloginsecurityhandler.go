// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package security

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/security"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func GetLoginSecurityHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := security.NewGetLoginSecurityLogic(r.Context(), svcCtx)
		resp, err := l.GetLoginSecurity()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
