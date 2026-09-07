//go:build integration

package authorization

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

func TestRolePolicyServiceReplacesLargeRoleInBoundedBatches(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	// The RPC accepts 10,000 selected IDs; the required API is added by the
	// service, so the persisted target must safely span three insert batches.
	apiIDs := seedBatchAuthorizationResources(t, ctx, db, 10000)
	notifier := &notifierStub{}
	service, err := NewRolePolicyService(db, notifier)
	if err != nil {
		t.Fatalf("create policy service: %v", err)
	}
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["a"].ID}); err != nil {
		t.Fatalf("write initial policies: %v", err)
	}
	installCasbinBatchAudit(t, ctx, db)

	result, err := service.ReplaceRoleAPIs(ctx, role.ID, apiIDs)
	if err != nil {
		t.Fatalf("replace large role policy: %v", err)
	}
	if result.Added != len(apiIDs) || result.Removed != 1 || result.NotificationError != nil {
		t.Fatalf("unexpected replacement result: %+v", result)
	}
	if notifier.calls.Load() != 2 {
		t.Fatalf("expected one notification per replacement, got %d", notifier.calls.Load())
	}
	stored, err := service.ListRoleAPIIDs(ctx, role.ID)
	if err != nil {
		t.Fatalf("list saved role APIs: %v", err)
	}
	want := append([]int64{resources["required"].ID}, apiIDs...)
	slices.Sort(want)
	if !slices.Equal(stored, want) {
		t.Fatalf("persisted API set differs from target: got %d IDs, want %d", len(stored), len(want))
	}

	var operations []string
	if err := db.WithContext(ctx).Table("casbin_batch_audit").Order("id").Pluck("operation", &operations).Error; err != nil {
		t.Fatalf("load write audit: %v", err)
	}
	if !slices.Equal(operations, []string{"DELETE", "INSERT", "INSERT", "INSERT"}) {
		t.Fatalf("unexpected batch write statements: %v", operations)
	}

	unchanged, err := service.ReplaceRoleAPIs(ctx, role.ID, apiIDs)
	if err != nil {
		t.Fatalf("repeat unchanged large policy: %v", err)
	}
	var statements int64
	if err := db.WithContext(ctx).Table("casbin_batch_audit").Count(&statements).Error; err != nil {
		t.Fatalf("count write audit: %v", err)
	}
	if unchanged.Changed() || notifier.calls.Load() != 2 || statements != 4 {
		t.Fatalf("unchanged policy caused effects: result=%+v notifications=%d statements=%d", unchanged, notifier.calls.Load(), statements)
	}
}

func TestRolePolicyServiceRollsBackEarlierBatchesWhenLaterBatchFails(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	apiIDs := seedBatchAuthorizationResources(t, ctx, db, 10000)
	notifier := &notifierStub{}
	service, err := NewRolePolicyService(db, notifier)
	if err != nil {
		t.Fatalf("create policy service: %v", err)
	}
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["a"].ID}); err != nil {
		t.Fatalf("write initial policies: %v", err)
	}
	before := loadRoleRules(t, db, role.ID)
	// Paths are ordered by the service. /batch/05000 is the first row of
	// batch two; a distinct SQLSTATE proves batch one really executed first.
	statements := []string{
		`CREATE FUNCTION fail_later_policy_batch() RETURNS trigger AS $$
		BEGIN
			IF NEW.v1 = '/batch/05000' THEN
				IF (SELECT count(*) FROM casbin_rule
					WHERE ptype = 'p' AND v0 = NEW.v0 AND v1 LIKE '/batch/%') <> 5000 THEN
					RAISE EXCEPTION 'first batch did not finish' USING ERRCODE = 'P0002';
				END IF;
				RAISE EXCEPTION 'forced later policy batch failure' USING ERRCODE = 'P0001';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`,
		`CREATE TRIGGER fail_later_policy_batch
		BEFORE INSERT ON casbin_rule
		FOR EACH ROW EXECUTE FUNCTION fail_later_policy_batch()`,
	}
	for _, statement := range statements {
		if err := db.WithContext(ctx).Exec(statement).Error; err != nil {
			t.Fatalf("install later-batch failure: %v", err)
		}
	}

	result, err := service.ReplaceRoleAPIs(ctx, role.ID, apiIDs)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "P0001" {
		t.Fatalf("expected injected failure after first batch, got %v", err)
	}
	if result.Changed() || result.NotificationError != nil || notifier.calls.Load() != 1 {
		t.Fatalf("failed replacement reported changes or notified: result=%+v notifications=%d", result, notifier.calls.Load())
	}
	after := loadRoleRules(t, db, role.ID)
	if !slices.Equal(after, before) {
		t.Fatalf("failed replacement did not restore original policies: before=%d after=%d", len(before), len(after))
	}
}

func seedBatchAuthorizationResources(t testing.TB, ctx context.Context, db *gorm.DB, count int) []int64 {
	t.Helper()
	resources := make([]model.API, count)
	for index := range resources {
		resources[index] = model.API{
			ServiceName: "system-api",
			Group:       "batch-test",
			Name:        fmt.Sprintf("Batch API %d", index),
			Path:        fmt.Sprintf("/batch/%05d", index),
			Method:      "POST",
			Status:      model.RecordStatusEnabled,
		}
	}
	// Keep fixture creation independent of the Casbin batch configuration.
	if err := db.WithContext(ctx).CreateInBatches(&resources, 1000).Error; err != nil {
		t.Fatalf("seed batch API resources: %v", err)
	}
	ids := make([]int64, len(resources))
	for index, resource := range resources {
		ids[index] = resource.ID
	}
	return ids
}

func installCasbinBatchAudit(t testing.TB, ctx context.Context, db *gorm.DB) {
	t.Helper()
	statements := []string{
		`CREATE TABLE casbin_batch_audit (id bigint GENERATED ALWAYS AS IDENTITY, operation text NOT NULL)`,
		`CREATE FUNCTION audit_casbin_batch() RETURNS trigger AS $$
		BEGIN
			INSERT INTO casbin_batch_audit(operation) VALUES (TG_OP);
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql`,
		`CREATE TRIGGER audit_casbin_batch
		AFTER INSERT OR DELETE ON casbin_rule
		FOR EACH STATEMENT EXECUTE FUNCTION audit_casbin_batch()`,
	}
	for _, statement := range statements {
		if err := db.WithContext(ctx).Exec(statement).Error; err != nil {
			t.Fatalf("install batch write audit: %v", err)
		}
	}
}
