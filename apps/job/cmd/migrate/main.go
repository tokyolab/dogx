package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/tokyolab/dogx/apps/job/internal/migration"
	"github.com/tokyolab/dogx/pkg/migratecmd"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	exitCode := migratecmd.Run(ctx, os.Args[1:], os.Stdout, os.Stderr, migrationOptions())
	stop()
	os.Exit(exitCode)
}

func migrationOptions() migratecmd.Options {
	return migratecmd.Options{
		CommandPath:         "./apps/job/cmd/migrate",
		DefaultConfigFile:   "apps/job/rpc/etc/job-rpc.yaml",
		DefaultMigrationDir: "apps/job/internal/migration/migrations",
		NewProvider:         migration.NewProvider,
	}
}
