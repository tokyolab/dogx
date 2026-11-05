package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/tokyolab/dogx/apps/system/internal/rpclog"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/server"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/rpchost"

	"github.com/zeromicro/go-zero/core/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/system-rpc.yaml", "the config file")

func main() {
	flag.Parse()
	// No production System job has been specified yet. Register concrete
	// processors here when their business behavior is defined.
	if err := run(context.Background(), *configFile, nil); err != nil {
		fmt.Fprintf(os.Stderr, "system RPC process failed: %v\n", err)
		os.Exit(1)
	}
}

// run assembles both System RPC servers and their shared process lifecycle.
// register injects business processors after System dependencies initialize;
// nil keeps the registry empty without changing server or shutdown behavior.
func run(processCtx context.Context, file string, register svc.RegisterProcessors) error {
	var c config.Config
	shutdown, err := rpchost.LoadConfig(file, &c)
	if err != nil {
		return err
	}
	if err := shutdown.Normalize(&c.ServiceConf); err != nil {
		return err
	}
	if err := c.PrepareExecutor(); err != nil {
		return err
	}
	rpclog.ProtectServerContent(&c.RpcServerConf)
	// The registration callback executes only when Run starts serving, after
	// the process settings and domain dependencies have been initialized.
	var ctx *svc.ServiceContext
	s, err := rpchost.NewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		system.RegisterSystemServer(grpcServer, server.NewSystemServer(ctx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	if err != nil {
		return err
	}
	s.AddUnaryInterceptors(bizerror.UnaryServerInterceptor)
	executor, err := jobexecutor.NewServer(c.JobExecutor.Server, func() *jobexecutor.RPCAdapter {
		return ctx.ExecutorAdapter
	})
	if err != nil {
		return err
	}
	fmt.Printf("Starting business RPC at %s and executor RPC at %s...\n", c.ListenOn, c.JobExecutor.Server.ListenOn)
	return rpchost.Run(processCtx, shutdown, rpchost.Hooks{
		Initialize: func(context.Context) error {
			var err error
			ctx, err = svc.NewServiceContext(c, register)
			return err
		},
		Stop:  func() { ctx.Stop() },
		Drain: func(wait context.Context) error { return ctx.Drain(wait) },
		Close: func() error { return ctx.Close() },
	}, s, executor)
}
