package main

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/pkg/migratecmd"
)

func TestMigrationOptionsBindJobDomain(t *testing.T) {
	options := migrationOptions()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := migratecmd.Run(context.Background(), []string{"-h"}, &stdout, &stderr, options); code != 0 {
		t.Fatalf("help exit code = %d: %s", code, stderr.String())
	}
	for _, want := range []string{
		"go run ./apps/job/cmd/migrate",
		"apps/job/rpc/etc/job-rpc.yaml",
		"apps/job/internal/migration/migrations",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("help output does not contain %q: %s", want, stderr.String())
		}
	}

	db, err := sql.Open("pgx", "host=localhost user=test dbname=test sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close migration test database: %v", err)
		}
	})
	provider, err := options.NewProvider(db)
	if err != nil {
		t.Fatalf("initialize Job provider: %v", err)
	}
	sources := provider.ListSources()
	if len(sources) == 0 || sources[0].Path != "20261016090000_init_job_trigger.sql" {
		t.Fatalf("command did not load Job migrations: %+v", sources)
	}
}
