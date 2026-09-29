// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package loginlog

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/loginlog"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// List login audit records
func ListLoginLogsHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.LoginLogListReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := loginlog.NewListLoginLogsLogic(r.Context(), svcCtx)
		resp, err := l.ListLoginLogs(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
