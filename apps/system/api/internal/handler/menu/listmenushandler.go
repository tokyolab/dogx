// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package menu

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/menu"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func ListMenusHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := menu.NewListMenusLogic(r.Context(), svcCtx)
		resp, err := l.ListMenus()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
