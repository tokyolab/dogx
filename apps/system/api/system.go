package main

import (
	"flag"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/handler"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/pkg/i18n"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	"github.com/tokyolab/dogx/pkg/response"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
)

var configFile = flag.String("f", "etc/system-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c, conf.UseEnv())

	server := rest.MustNewServer(
		c.RestConf,
		rest.WithUnauthorizedCallback(response.HandleUnauthorized),
	)
	defer server.Stop()

	svcCtx, err := svc.NewServiceContext(c)
	logx.Must(err)
	defer func() {
		if err := svcCtx.Close(); err != nil {
			logx.Errorf("close service context: %v", err)
		}
	}()
	httpx.SetOkHandler(response.HandleSuccess)
	httpx.SetErrorHandlerCtx(response.HandleError)
	httpx.SetValidator(requestvalidator.New())
	server.Use(i18n.Middleware)
	handler.RegisterHandlers(server, svcCtx)

	fmt.Printf("Starting %s at %s:%d\n", c.Name, c.Host, c.Port)
	server.Start()
}
