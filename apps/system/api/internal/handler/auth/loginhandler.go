// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package auth

import (
	"net"
	"net/http"
	"strings"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/auth"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// Sign in with username and password
func LoginHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.LoginReq
		if err := httpx.Parse(r, &req); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}

		l := auth.NewLoginLogic(r.Context(), svcCtx)
		resp, err := l.Login(&req, auth.LoginMetadata{
			IPAddress: clientIPAddress(r),
			UserAgent: r.UserAgent(),
		})
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		} else {
			httpx.OkJsonCtx(r.Context(), w, resp)
		}
	}
}

func clientIPAddress(r *http.Request) string {
	value := httpx.GetRemoteAddr(r)
	if separator := strings.IndexByte(value, ','); separator >= 0 {
		value = value[:separator]
	}
	value = strings.TrimSpace(value)
	if host, _, err := net.SplitHostPort(value); err == nil {
		value = host
	}
	value = strings.Trim(value, "[]")
	if ip := net.ParseIP(value); ip != nil {
		return ip.String()
	}
	return ""
}
