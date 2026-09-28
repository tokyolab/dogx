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

func UpdateDictionaryHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.UpdateDictionaryReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := dictionary.NewUpdateDictionaryLogic(r.Context(), svcCtx)
		resp, err := l.UpdateDictionary(&req)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
