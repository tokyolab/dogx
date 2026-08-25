//go:build integration

package migratecmd

import (
	"bytes"
	"context"
	"strconv"
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
	sources := provider.ListSources()
	if len(sources) == 0 {
		t.Fatal("migration provider has no sources")
	}

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
	assertCommand("up", sources[len(sources)-1].Path)
	assertCommand("up", "no pending migrations")
	assertCommand("version", strconv.FormatInt(sources[len(sources)-1].Version, 10))
	assertCommand("status", "applied")
	for i := len(sources) - 1; i >= 0; i-- {
		assertCommand("down", sources[i].Path)
	}
	assertCommand("down", "no applied migrations")
}
