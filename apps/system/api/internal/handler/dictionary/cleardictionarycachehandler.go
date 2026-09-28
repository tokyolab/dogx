// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package dictionary

import (
	"net/http"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/dictionary"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"

	"github.com/zeromicro/go-zero/rest/httpx"
)

func ClearDictionaryCacheHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := dictionary.NewClearDictionaryCacheLogic(r.Context(), svcCtx)
		resp, err := l.ClearDictionaryCache()
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}
