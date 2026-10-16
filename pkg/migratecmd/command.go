package migratecmd

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/tokyolab/dogx/pkg/postgres"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/zeromicro/go-zero/core/conf"
)

const (
	exitSuccess = 0
	exitFailure = 1
	exitUsage   = 2
)

type Options struct {
	CommandPath         string
	DefaultConfigFile   string
	DefaultMigrationDir string
	// Database commands use the provider's embedded sources and ledger;
	// DefaultMigrationDir is used only by create and fix.
	NewProvider func(*sql.DB) (*goose.Provider, error)
}

type migrateConfig struct {
	Postgres postgres.Config
}

// Run executes the migration command and returns a process exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) int {
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	flags.SetOutput(stderr)

	configFile := flags.String("f", options.DefaultConfigFile, "path to the RPC configuration file")
	migrationDir := flags.String("dir", options.DefaultMigrationDir, "path to the migration source directory")
	flags.Usage = func() {
		writeUsage(flags, stderr, options.CommandPath)
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

	if err := executeDatabaseCommand(ctx, *configFile, command, stdout, options.NewProvider); err != nil {
		return reportError(stderr, err)
	}
	return exitSuccess
}

func writeUsage(flags *flag.FlagSet, output io.Writer, commandPath string) {
	_, _ = fmt.Fprintln(output, "Usage:")
	_, _ = fmt.Fprintf(output, "  go run %s [flags] <up|down|status|version>\n", commandPath)
	_, _ = fmt.Fprintf(output, "  go run %s [flags] create <name>\n", commandPath)
	_, _ = fmt.Fprintf(output, "  go run %s [flags] fix\n", commandPath)
	flags.PrintDefaults()
}

func executeDatabaseCommand(ctx context.Context, configFile, command string, output io.Writer, newProvider func(*sql.DB) (*goose.Provider, error)) (resultErr error) {
	var cfg migrateConfig
	if err := conf.Load(configFile, &cfg, conf.UseEnv()); err != nil {
		return fmt.Errorf("load migration config: %w", err)
	}

	db, err := sql.Open("pgx", cfg.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close postgres: %w", err))
		}
	}()

	provider, err := newProvider(db)
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
