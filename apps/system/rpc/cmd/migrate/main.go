package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/config"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/migration"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/zeromicro/go-zero/core/conf"
)

const (
	defaultConfigFile   = "apps/system/rpc/etc/system-rpc.yaml"
	defaultMigrationDir = "apps/system/rpc/internal/migration/migrations"
)

type migrateConfig struct {
	Postgres config.PostgresConf
}

func main() {
	configFile := flag.String("f", defaultConfigFile, "path to the RPC configuration file")
	migrationDir := flag.String("dir", defaultMigrationDir, "path to the migration source directory")
	flag.Usage = func() {
		_, _ = fmt.Fprintln(flag.CommandLine.Output(), "Usage:")
		_, _ = fmt.Fprintln(flag.CommandLine.Output(), "  go run ./apps/system/rpc/cmd/migrate [flags] <up|down|status|version>")
		_, _ = fmt.Fprintln(flag.CommandLine.Output(), "  go run ./apps/system/rpc/cmd/migrate [flags] create <name>")
		_, _ = fmt.Fprintln(flag.CommandLine.Output(), "  go run ./apps/system/rpc/cmd/migrate [flags] fix")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	command := flag.Arg(0)
	switch command {
	case "create":
		if flag.NArg() != 2 {
			flag.Usage()
			os.Exit(2)
		}
		goose.SetSequential(false)
		if err := goose.Create(nil, *migrationDir, flag.Arg(1), "sql"); err != nil {
			exitWithError(fmt.Errorf("create migration: %w", err))
		}
		return
	case "fix":
		if flag.NArg() != 1 {
			flag.Usage()
			os.Exit(2)
		}
		if err := goose.Fix(*migrationDir); err != nil {
			exitWithError(fmt.Errorf("normalize migration versions: %w", err))
		}
		fmt.Println("migration filenames normalized")
		return
	}

	if flag.NArg() != 1 || !isDatabaseCommand(command) {
		_, _ = fmt.Fprintf(os.Stderr, "unsupported migration command %q\n", command)
		flag.Usage()
		os.Exit(2)
	}

	var cfg migrateConfig
	if err := conf.Load(*configFile, &cfg, conf.UseEnv()); err != nil {
		exitWithError(fmt.Errorf("load migration config: %w", err))
	}

	db, err := sql.Open("pgx", cfg.Postgres.DSN())
	if err != nil {
		exitWithError(fmt.Errorf("open postgres: %w", err))
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	provider, err := migration.NewProvider(db)
	if err != nil {
		exitWithError(err)
	}
	if err := provider.Ping(ctx); err != nil {
		exitWithError(fmt.Errorf("connect postgres: %w", err))
	}

	if err := runCommand(ctx, provider, command); err != nil {
		exitWithError(err)
	}
}

func isDatabaseCommand(command string) bool {
	switch command {
	case "up", "down", "status", "version":
		return true
	default:
		return false
	}
}

func runCommand(ctx context.Context, provider *goose.Provider, command string) error {
	switch command {
	case "up":
		results, err := provider.Up(ctx)
		if err != nil {
			return fmt.Errorf("apply migrations: %w", err)
		}
		if len(results) == 0 {
			fmt.Println("no pending migrations")
			return nil
		}
		for _, result := range results {
			fmt.Println(result)
		}
	case "down":
		result, err := provider.Down(ctx)
		if err != nil {
			if errors.Is(err, goose.ErrNoNextVersion) {
				fmt.Println("no applied migrations")
				return nil
			}
			return fmt.Errorf("rollback migration: %w", err)
		}
		fmt.Println(result)
	case "status":
		statuses, err := provider.Status(ctx)
		if err != nil {
			return fmt.Errorf("read migration status: %w", err)
		}
		for _, status := range statuses {
			fmt.Printf("%-8s %05d %s\n", status.State, status.Source.Version, status.Source.Path)
		}
	case "version":
		version, err := provider.GetDBVersion(ctx)
		if err != nil {
			return fmt.Errorf("read migration version: %w", err)
		}
		fmt.Println(version)
	}

	return nil
}

func exitWithError(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
