// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package department

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/department"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func ListDepartmentsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := department.NewListDepartmentsLogic(r.Context(), svcCtx)
		resp, err := l.ListDepartments()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
