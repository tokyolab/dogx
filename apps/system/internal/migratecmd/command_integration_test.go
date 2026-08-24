//go:build integration

package migratecmd

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
)

func TestRunCommandLifecycleInPostgreSQL(t *testing.T) {
	_, sqlDB := testutil.OpenPostgres(t)
	provider, err := migration.NewProvider(sqlDB)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	assertCommand := func(command, wantContain string) {
		t.Helper()

		var output bytes.Buffer
		if err := runCommand(ctx, provider, command, &output); err != nil {
			t.Fatalf("run %s: %v", command, err)
		}
		if !strings.Contains(strings.ToLower(output.String()), strings.ToLower(wantContain)) {
			t.Fatalf("output for %s = %q, want substring %q", command, output.String(), wantContain)
		}
	}

	assertCommand("status", "pending")
	assertCommand("up", "00001_init_system.sql")
	assertCommand("up", "no pending migrations")
	assertCommand("version", "1")
	assertCommand("status", "applied")
	assertCommand("down", "00001_init_system.sql")
	assertCommand("down", "no applied migrations")
}
