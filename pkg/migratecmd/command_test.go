package migratecmd

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/pressly/goose/v3"
)

func TestRunRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing command"},
		{name: "unsupported command", args: []string{"drop"}},
		{name: "create without name", args: []string{"create"}},
		{name: "create with extra argument", args: []string{"create", "name", "extra"}},
		{name: "fix with extra argument", args: []string{"fix", "extra"}},
		{name: "database command with extra argument", args: []string{"up", "extra"}},
		{name: "invalid flag", args: []string{"-unknown"}},
		{name: "flag without value", args: []string{"-f"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer

			if code := Run(context.Background(), test.args, &stdout, &stderr, testOptions()); code != exitUsage {
				t.Fatalf("exit code = %d, want %d", code, exitUsage)
			}
			if stderr.Len() == 0 {
				t.Fatal("expected usage information on stderr")
			}
		})
	}
}

func TestRunHelpSucceeds(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	options := testOptions()
	if code := Run(context.Background(), []string{"-h"}, &stdout, &stderr, options); code != exitSuccess {
		t.Fatalf("exit code = %d, want %d", code, exitSuccess)
	}
	for _, want := range []string{"Usage:", options.CommandPath, options.DefaultConfigFile, options.DefaultMigrationDir} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("help output does not contain %q: %s", want, stderr.String())
		}
	}
}

func TestRunReportsConfigurationFailure(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	missingConfig := filepath.Join(t.TempDir(), "missing.yaml")

	code := Run(
		context.Background(),
		[]string{"-f", missingConfig, "up"},
		&stdout,
		&stderr,
		testOptions(),
	)
	if code != exitFailure {
		t.Fatalf("exit code = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "load migration config") {
		t.Fatalf("unexpected error output: %s", stderr.String())
	}
}

func TestRunReportsDatabaseConnectionFailure(t *testing.T) {
	configFile := writeTestConfig(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(ctx, []string{"-f", configFile, "up"}, &stdout, &stderr, testOptions())
	if code != exitFailure {
		t.Fatalf("exit code = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "connect postgres") {
		t.Fatalf("unexpected error output: %s", stderr.String())
	}
}

func TestRunCreatesMigrationFile(t *testing.T) {
	directory := t.TempDir()
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(
		context.Background(),
		[]string{"-dir", directory, "create", "add_audit_log"},
		&stdout,
		&stderr,
		testOptions(),
	)
	if code != exitSuccess {
		t.Fatalf("exit code = %d, want %d: %s", code, exitSuccess, stderr.String())
	}

	files, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read migration directory: %v", err)
	}
	if len(files) != 1 || !strings.HasSuffix(files[0].Name(), "_add_audit_log.sql") {
		t.Fatalf("unexpected created migrations: %+v", files)
	}
	if _, err := time.Parse("20060102150405", strings.TrimSuffix(files[0].Name(), "_add_audit_log.sql")); err != nil {
		t.Fatalf("created migration must use a timestamp version: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(directory, files[0].Name()))
	if err != nil {
		t.Fatalf("read created migration: %v", err)
	}
	if !bytes.Contains(content, []byte("-- +goose Up")) || !bytes.Contains(content, []byte("-- +goose Down")) {
		t.Fatalf("created migration does not contain Goose sections:\n%s", content)
	}
}

func TestRunFixNormalizesMigrationVersions(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{
		"00001_published.sql",
		"20260824110000_first.sql",
		"20260824120000_second.sql",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("-- migration\n"), 0o600); err != nil {
			t.Fatalf("write migration %s: %v", name, err)
		}
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(context.Background(), []string{"-dir", directory, "fix"}, &stdout, &stderr, testOptions())
	if code != exitSuccess {
		t.Fatalf("exit code = %d, want %d: %s", code, exitSuccess, stderr.String())
	}
	if !strings.Contains(stdout.String(), "migration filenames normalized") {
		t.Fatalf("unexpected output: %s", stdout.String())
	}

	files, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read normalized migrations: %v", err)
	}
	got := make([]string, 0, len(files))
	for _, file := range files {
		got = append(got, file.Name())
	}
	want := []string{"00001_published.sql", "00002_first.sql", "00003_second.sql"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("normalized migrations = %v, want %v", got, want)
	}
	for _, name := range want {
		content, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Fatalf("read normalized migration %s: %v", name, err)
		}
		if string(content) != "-- migration\n" {
			t.Errorf("migration %s content changed: %q", name, content)
		}
	}
}

func TestRunReportsMigrationFileOperationFailures(t *testing.T) {
	notDirectory := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(notDirectory, []byte("file"), 0o600); err != nil {
		t.Fatalf("write path blocker: %v", err)
	}

	tests := []struct {
		name       string
		args       []string
		wantPrefix string
	}{
		{
			name:       "create",
			args:       []string{"-dir", notDirectory, "create", "add_audit_log"},
			wantPrefix: "create migration",
		},
		{
			name:       "fix",
			args:       []string{"-dir", notDirectory, "fix"},
			wantPrefix: "normalize migration versions",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if code := Run(context.Background(), test.args, &stdout, &stderr, testOptions()); code != exitFailure {
				t.Fatalf("exit code = %d, want %d", code, exitFailure)
			}
			if !strings.Contains(stderr.String(), test.wantPrefix) {
				t.Fatalf("unexpected error output: %s", stderr.String())
			}
		})
	}
}

func TestRunUsesDefaultMigrationDirectoryWithoutDatabase(t *testing.T) {
	options := testOptions()
	options.DefaultMigrationDir = t.TempDir()
	options.NewProvider = nil
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run(context.Background(), []string{"create", "default_path"}, &stdout, &stderr, options); code != exitSuccess {
		t.Fatalf("create exit code = %d: %s", code, stderr.String())
	}
	if code := Run(context.Background(), []string{"fix"}, &stdout, &stderr, options); code != exitSuccess {
		t.Fatalf("fix exit code = %d: %s", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(options.DefaultMigrationDir, "00001_default_path.sql")); err != nil {
		t.Fatalf("migration not created and normalized in default directory: %v", err)
	}
}

func TestRunReportsProviderFailureAndClosesDatabase(t *testing.T) {
	options := testOptions()
	options.DefaultConfigFile = writeTestConfig(t)
	wantErr := errors.New("provider initialization failed")
	var openedDB *sql.DB
	options.NewProvider = func(db *sql.DB) (*goose.Provider, error) {
		openedDB = db
		return nil, wantErr
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := Run(context.Background(), []string{"up"}, &stdout, &stderr, options); code != exitFailure {
		t.Fatalf("exit code = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), wantErr.Error()) {
		t.Fatalf("unexpected error output: %s", stderr.String())
	}
	if openedDB == nil {
		t.Fatal("provider factory was not called with the configured database")
	}
	if err := openedDB.PingContext(context.Background()); err == nil || !strings.Contains(err.Error(), "database is closed") {
		t.Fatalf("database must close after provider failure, got %v", err)
	}
}

func TestRunCommandReportsDatabaseErrors(t *testing.T) {
	db, err := sql.Open("pgx", "host=localhost user=test dbname=test sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := newTestProvider(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		command string
		prefix  string
	}{
		{"up", "apply migrations"},
		{"down", "rollback migration"},
		{"status", "read migration status"},
		{"version", "read migration version"},
	} {
		t.Run(test.command, func(t *testing.T) {
			var output bytes.Buffer
			err := runCommand(context.Background(), provider, test.command, &output)
			if err == nil || !strings.Contains(err.Error(), test.prefix) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func testOptions() Options {
	return Options{
		CommandPath:         "./apps/example/cmd/migrate",
		DefaultConfigFile:   "example-rpc.yaml",
		DefaultMigrationDir: "example/migrations",
		NewProvider:         newTestProvider,
	}
}

func newTestProvider(db *sql.DB) (*goose.Provider, error) {
	return goose.NewProvider(goose.DialectPostgres, db, fstest.MapFS{
		"00001_create_example.sql": {Data: []byte(`-- +goose Up
CREATE TABLE migration_example (id BIGINT PRIMARY KEY);
-- +goose Down
DROP TABLE migration_example;
`)},
		"00002_seed_example.sql": {Data: []byte(`-- +goose Up
INSERT INTO migration_example (id) VALUES (1);
-- +goose Down
DELETE FROM migration_example WHERE id = 1;
`)},
	})
}

func writeTestConfig(t *testing.T) string {
	t.Helper()
	configFile := filepath.Join(t.TempDir(), "example-rpc.yaml")
	config := []byte(`Postgres:
  Host: 127.0.0.1
  Port: 1
  User: dogx
  Password: invalid
  Database: dogx_test
  SSLMode: disable
  TimeZone: Asia/Shanghai
  MaxIdleConns: 1
  MaxOpenConns: 1
  ConnMaxLifetime: 1m
`)
	if err := os.WriteFile(configFile, config, 0o600); err != nil {
		t.Fatalf("write migration config: %v", err)
	}
	return configFile
}

func TestDatabaseCommandRecognition(t *testing.T) {
	for _, command := range []string{"up", "down", "status", "version"} {
		if !isDatabaseCommand(command) {
			t.Errorf("expected %q to be a database command", command)
		}
	}
	for _, command := range []string{"", "create", "fix", "drop"} {
		if isDatabaseCommand(command) {
			t.Errorf("did not expect %q to be a database command", command)
		}
	}
}

func TestRunCommandRejectsUnknownCommand(t *testing.T) {
	var output bytes.Buffer
	err := runCommand(context.Background(), nil, "drop", &output)
	if err == nil || !strings.Contains(err.Error(), "unsupported database command") {
		t.Fatalf("unexpected error: %v", err)
	}
}
