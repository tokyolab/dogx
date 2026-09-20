// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package department

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/department"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func UpdateDepartmentStatusHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.UpdateDepartmentStatusReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := department.NewUpdateDepartmentStatusLogic(r.Context(), svcCtx)
		resp, err := l.UpdateDepartmentStatus(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
