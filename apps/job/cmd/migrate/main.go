package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/tokyolab/dogx/apps/job/internal/migratecmd"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	exitCode := migratecmd.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(exitCode)
}
