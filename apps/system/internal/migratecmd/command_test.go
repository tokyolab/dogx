package migratecmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRejectsInvalidArguments(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing command"},
		{name: "unsupported command", args: []string{"drop"}},
		{name: "create without name", args: []string{"create"}},
		{name: "fix with extra argument", args: []string{"fix", "extra"}},
		{name: "database command with extra argument", args: []string{"up", "extra"}},
		{name: "invalid flag", args: []string{"-unknown"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer

			if code := Run(context.Background(), test.args, &stdout, &stderr); code != exitUsage {
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

	if code := Run(context.Background(), []string{"-h"}, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit code = %d, want %d", code, exitSuccess)
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Fatalf("help output does not contain usage: %s", stderr.String())
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
	)
	if code != exitFailure {
		t.Fatalf("exit code = %d, want %d", code, exitFailure)
	}
	if !strings.Contains(stderr.String(), "load migration config") {
		t.Fatalf("unexpected error output: %s", stderr.String())
	}
}

func TestRunReportsDatabaseConnectionFailure(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "system-rpc.yaml")
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

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(ctx, []string{"-f", configFile, "up"}, &stdout, &stderr)
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
		"20260824110000_first.sql",
		"20260824120000_second.sql",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("-- migration\n"), 0o600); err != nil {
			t.Fatalf("write migration %s: %v", name, err)
		}
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(context.Background(), []string{"-dir", directory, "fix"}, &stdout, &stderr)
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
	want := []string{"00001_first.sql", "00002_second.sql"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("normalized migrations = %v, want %v", got, want)
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
			if code := Run(context.Background(), test.args, &stdout, &stderr); code != exitFailure {
				t.Fatalf("exit code = %d, want %d", code, exitFailure)
			}
			if !strings.Contains(stderr.String(), test.wantPrefix) {
				t.Fatalf("unexpected error output: %s", stderr.String())
			}
		})
	}
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
