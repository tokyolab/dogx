package health

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/health"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func ReadyHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logic := health.NewReadyLogic(r.Context(), svcCtx)
		response, err := logic.Ready()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		httpx.OkJsonCtx(r.Context(), w, response)
	}
}
