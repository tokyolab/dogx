package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/tokyolab/dogx/apps/system/internal/migration"
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
		CommandPath:         "./apps/system/cmd/migrate",
		DefaultConfigFile:   "apps/system/rpc/etc/system-rpc.yaml",
		DefaultMigrationDir: "apps/system/internal/migration/migrations",
		NewProvider:         migration.NewProvider,
	}
}
