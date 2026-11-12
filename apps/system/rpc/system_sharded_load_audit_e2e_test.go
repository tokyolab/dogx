//go:build integration && e2e && load

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor"
	testpostgres "github.com/tokyolab/dogx/pkg/testutil/postgres"
	"gorm.io/gorm"
)

// TestShardedLoadAuditSurvivesBusinessCancellation forces cancellation at the
// GORM update boundary, after perform has finished its business wait and selected
// the audit Context. Reusing the business Context loses the terminal audit row.
func TestShardedLoadAuditSurvivesBusinessCancellation(t *testing.T) {
	db, sqlDB := testpostgres.OpenPostgres(t)
	setup, stopSetup := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopSetup()
	_, err := sqlDB.ExecContext(setup, `CREATE SEQUENCE system_job_load_invocations;
		CREATE TABLE system_job_e2e_records (
			task_id BIGINT NOT NULL,
			invocation_no BIGINT NOT NULL DEFAULT nextval('system_job_load_invocations'),
			instance_id BIGINT NOT NULL, job_id BIGINT NOT NULL,
			value TEXT NOT NULL, state TEXT NOT NULL,
			started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			finished_at TIMESTAMPTZ,
			PRIMARY KEY (task_id, invocation_no))`)
	if err != nil {
		t.Fatalf("create isolated load audit fixture: %v", err)
	}
	business, cancelBusiness := context.WithCancel(context.Background())
	defer cancelBusiness()
	const callback = "test:cancel_business_before_load_audit"
	called, auditCanceled := false, false
	if err := db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "system_job_e2e_records" {
			called = true
			cancelBusiness()
			auditCanceled = tx.Statement.Context.Err() != nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Callback().Update().Remove(callback); err != nil {
			t.Errorf("remove load audit callback: %v", err)
		}
	})
	processor := &systemShardedLoadProcessor{db: db, worker: "audit-race-test"}
	_, err = processor.Execute(business, jobexecutor.Input{JobID: 1, InstanceID: 2, TaskID: 3,
		Params: `{"count":0,"payload_bytes":0,"delay_ms":100}`})
	if !called || auditCanceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("audit cancellation boundary: called=%t audit_canceled=%t result=%v", called, auditCanceled, err)
	}
	read, stopRead := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopRead()
	var state string
	var finished bool
	if err := sqlDB.QueryRowContext(read, `SELECT state, finished_at IS NOT NULL
		FROM system_job_e2e_records WHERE task_id = 3`).Scan(&state, &finished); err != nil {
		t.Fatalf("read terminal load audit: %v", err)
	}
	if state != "succeeded" || !finished {
		t.Fatalf("completed business audit remained unfinished: state=%s finished=%t", state, finished)
	}
}
