//go:build integration

package authorization

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestRoleDeletionWaitingForPolicyWriterCannotMissNewPolicies(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	service, err := NewRolePolicyService(db, &notifierStub{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	writerReady := make(chan struct{}, 1)
	releaseWriter := make(chan struct{})
	deletePID := make(chan int, 1)
	type deletingContextKey struct{}
	if err := db.Callback().Create().Before("gorm:create").Register("test:hold-policy-writer", func(tx *gorm.DB) {
		if tx.Statement.Table != "casbin_rule" {
			return
		}
		writerReady <- struct{}{}
		select {
		case <-releaseWriter:
		case <-ctx.Done():
			_ = tx.AddError(ctx.Err())
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Callback().Query().Before("gorm:query").Register("test:identify-deleting-transaction", func(tx *gorm.DB) {
		if tx.Statement.Table != "sys_role" || tx.Statement.Context.Value(deletingContextKey{}) != true {
			return
		}
		// Identify this connection so the test can observe a real row-lock wait.
		var pid int
		if err := tx.Statement.ConnPool.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			_ = tx.AddError(err)
			return
		}
		deletePID <- pid
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Callback().Create().Remove("test:hold-policy-writer")
		_ = db.Callback().Query().Remove("test:identify-deleting-transaction")
	})
	writerResult := make(chan error, 1)
	go func() {
		_, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["a"].ID})
		writerResult <- err
	}()
	select {
	case <-writerReady:
	case <-ctx.Done():
		t.Fatal("policy writer did not acquire the role lock")
	}
	deleteResult := make(chan error, 1)
	go func() {
		_, err := service.DeleteRole(context.WithValue(ctx, deletingContextKey{}, true), role.ID)
		deleteResult <- err
	}()
	var pid int
	select {
	case pid = <-deletePID:
	case <-ctx.Done():
		t.Fatal("deleting transaction did not start")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blockers int
		if err := db.WithContext(ctx).Raw("SELECT cardinality(pg_blocking_pids(?))", pid).Scan(&blockers).Error; err != nil {
			t.Fatal(err)
		}
		if blockers > 0 {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("deletion did not wait for the policy writer")
		}
	}
	close(releaseWriter)
	if err := <-writerResult; err != nil {
		t.Fatalf("policy writer failed: %v", err)
	}
	if err := <-deleteResult; err != nil {
		t.Fatalf("deletion after policy writer committed: %v", err)
	}
	if got := loadRoleRules(t, db, role.ID); len(got) != 0 {
		t.Fatalf("deleted role retained policies: %+v", got)
	}
}
