//go:build integration

package authorization

import (
	"context"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/testutil"
)

func TestReadinessChecksLoadedPolicyAndPostgreSQL(t *testing.T) {
	_, sqlDB := testutil.OpenPostgres(t)
	reloader, err := NewPolicyReloader(&policyLoaderStub{})
	if err != nil {
		t.Fatalf("create policy reloader: %v", err)
	}
	if err := reloader.Reload(); err != nil {
		t.Fatalf("mark initial policy loaded: %v", err)
	}
	checker, err := NewReadiness(sqlDB, reloader)
	if err != nil {
		t.Fatalf("create authorization readiness: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := checker.Check(ctx); err != nil {
		t.Fatalf("ready authorization dependencies failed: %v", err)
	}

	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close readiness database: %v", err)
	}
	if err := checker.Check(ctx); err == nil {
		t.Fatal("closed PostgreSQL pool was reported ready")
	}
}
