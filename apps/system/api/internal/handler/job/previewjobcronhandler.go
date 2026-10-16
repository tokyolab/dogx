// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package job

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/job"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

func PreviewJobCronHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.PreviewJobCronReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := job.NewPreviewJobCronLogic(r.Context(), svcCtx)
		resp, err := l.PreviewJobCron(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
