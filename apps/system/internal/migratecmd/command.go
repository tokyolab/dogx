package migratecmd

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/tokyolab/dogx/apps/system/internal/database"
	"github.com/tokyolab/dogx/apps/system/internal/migration"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/zeromicro/go-zero/core/conf"
)

const (
	defaultConfigFile   = "apps/system/rpc/etc/system-rpc.yaml"
	defaultMigrationDir = "apps/system/internal/migration/migrations"

	exitSuccess = 0
	exitFailure = 1
	exitUsage   = 2
)

type migrateConfig struct {
	Postgres database.PostgresConf
}

// Run executes the migration command and returns a process exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	flags.SetOutput(stderr)

	configFile := flags.String("f", defaultConfigFile, "path to the RPC configuration file")
	migrationDir := flags.String("dir", defaultMigrationDir, "path to the migration source directory")
	flags.Usage = func() {
		writeUsage(flags, stderr)
	}

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitSuccess
		}
		return exitUsage
	}
	if flags.NArg() == 0 {
		flags.Usage()
		return exitUsage
	}

	command := flags.Arg(0)
	switch command {
	case "create":
		if flags.NArg() != 2 {
			flags.Usage()
			return exitUsage
		}
		goose.SetSequential(false)
		if err := goose.Create(nil, *migrationDir, flags.Arg(1), "sql"); err != nil {
			return reportError(stderr, fmt.Errorf("create migration: %w", err))
		}
		return exitSuccess
	case "fix":
		if flags.NArg() != 1 {
			flags.Usage()
			return exitUsage
		}
		if err := goose.Fix(*migrationDir); err != nil {
			return reportError(stderr, fmt.Errorf("normalize migration versions: %w", err))
		}
		_, _ = fmt.Fprintln(stdout, "migration filenames normalized")
		return exitSuccess
	}

	if flags.NArg() != 1 || !isDatabaseCommand(command) {
		_, _ = fmt.Fprintf(stderr, "unsupported migration command %q\n", command)
		flags.Usage()
		return exitUsage
	}

	if err := executeDatabaseCommand(ctx, *configFile, command, stdout); err != nil {
		return reportError(stderr, err)
	}
	return exitSuccess
}

func writeUsage(flags *flag.FlagSet, output io.Writer) {
	_, _ = fmt.Fprintln(output, "Usage:")
	_, _ = fmt.Fprintln(output, "  go run ./apps/system/cmd/migrate [flags] <up|down|status|version>")
	_, _ = fmt.Fprintln(output, "  go run ./apps/system/cmd/migrate [flags] create <name>")
	_, _ = fmt.Fprintln(output, "  go run ./apps/system/cmd/migrate [flags] fix")
	flags.PrintDefaults()
}

func executeDatabaseCommand(ctx context.Context, configFile, command string, output io.Writer) error {
	var cfg migrateConfig
	if err := conf.Load(configFile, &cfg, conf.UseEnv()); err != nil {
		return fmt.Errorf("load migration config: %w", err)
	}

	db, err := sql.Open("pgx", cfg.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	provider, err := migration.NewProvider(db)
	if err != nil {
		return err
	}
	if err := provider.Ping(ctx); err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}

	return runCommand(ctx, provider, command, output)
}

func isDatabaseCommand(command string) bool {
	switch command {
	case "up", "down", "status", "version":
		return true
	default:
		return false
	}
}

func runCommand(ctx context.Context, provider *goose.Provider, command string, output io.Writer) error {
	switch command {
	case "up":
		results, err := provider.Up(ctx)
		if err != nil {
			return fmt.Errorf("apply migrations: %w", err)
		}
		if len(results) == 0 {
			_, _ = fmt.Fprintln(output, "no pending migrations")
			return nil
		}
		for _, result := range results {
			_, _ = fmt.Fprintln(output, result)
		}
	case "down":
		result, err := provider.Down(ctx)
		if err != nil {
			if errors.Is(err, goose.ErrNoNextVersion) {
				_, _ = fmt.Fprintln(output, "no applied migrations")
				return nil
			}
			return fmt.Errorf("rollback migration: %w", err)
		}
		_, _ = fmt.Fprintln(output, result)
	case "status":
		statuses, err := provider.Status(ctx)
		if err != nil {
			return fmt.Errorf("read migration status: %w", err)
		}
		for _, status := range statuses {
			_, _ = fmt.Fprintf(output, "%-8s %05d %s\n", status.State, status.Source.Version, status.Source.Path)
		}
	case "version":
		version, err := provider.GetDBVersion(ctx)
		if err != nil {
			return fmt.Errorf("read migration version: %w", err)
		}
		_, _ = fmt.Fprintln(output, version)
	default:
		return fmt.Errorf("unsupported database command %q", command)
	}

	return nil
}

func reportError(output io.Writer, err error) int {
	_, _ = fmt.Fprintln(output, err)
	return exitFailure
}
