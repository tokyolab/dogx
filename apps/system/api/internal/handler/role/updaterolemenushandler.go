// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package role

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/role"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// Replace the complete PC menu authorization set for a role
func UpdateRoleMenusHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.UpdateRoleMenusReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := role.NewUpdateRoleMenusLogic(r.Context(), svcCtx)
		resp, err := l.UpdateRoleMenus(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
