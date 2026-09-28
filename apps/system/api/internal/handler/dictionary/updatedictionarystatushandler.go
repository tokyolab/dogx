// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package dictionary

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/dictionary"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func UpdateDictionaryStatusHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.UpdateDictionaryStatusReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := dictionary.NewUpdateDictionaryStatusLogic(r.Context(), svcCtx)
		resp, err := l.UpdateDictionaryStatus(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
