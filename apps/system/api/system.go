package main

import (
	"flag"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/handler"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/pkg/response"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
)

var configFile = flag.String("f", "etc/system-api.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c, conf.UseEnv())

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	svcCtx := svc.NewServiceContext(c)
	httpx.SetOkHandler(response.HandleSuccess)
	httpx.SetErrorHandlerCtx(response.HandleError)
	handler.RegisterHandlers(server, svcCtx)

	fmt.Printf("Starting %s at %s:%d\n", c.Name, c.Host, c.Port)
	server.Start()
}
