package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/tokyolab/dogx/apps/job/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/execution"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/server"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/rpchost"

	"github.com/zeromicro/go-zero/core/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/job-rpc.yaml", "the config file")

func main() {
	flag.Parse()
	if err := run(context.Background(), *configFile); err != nil {
		// Reporting a process failure must not reopen global logging after cleanup.
		fmt.Fprintf(os.Stderr, "job RPC process failed: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, file string) error {
	var c config.Config
	shutdown, err := rpchost.LoadConfig(file, &c)
	if err != nil {
		return err
	}
	if err := shutdown.Normalize(&c.ServiceConf); err != nil {
		return err
	}
	if err := c.PrepareExecution(); err != nil {
		return err
	}
	config.ProtectServerContent(&c.RpcServerConf)
	// Construction sets up the shared process settings but does not listen.
	// The callback runs only after dependency initialization and host startup.
	var services *svc.ServiceContext
	s, err := rpchost.NewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		job.RegisterJobServer(grpcServer, server.NewJobServer(services))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	if err != nil {
		return err
	}
	s.AddUnaryInterceptors(bizerror.UnaryServerInterceptor)
	results, err := execution.NewServer(c.ExecutionRPC, func() *execution.Receiver {
		return services.ResultReceiver
	})
	if err != nil {
		return err
	}
	fmt.Printf("Starting management RPC at %s and execution RPC at %s...\n", c.ListenOn, c.ExecutionRPC.ListenOn)
	return rpchost.Run(ctx, shutdown, rpchost.Hooks{
		Initialize: func(context.Context) error {
			var err error
			services, err = svc.NewServiceContext(c)
			return err
		},
		Stop:  func() { services.Stop() },
		Drain: func(wait context.Context) error { return services.Drain(wait) },
		Close: func() error { return services.Close() },
	}, s, results)
}
