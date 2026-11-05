//go:build integration

package migration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
)

func TestTaskSingletonConstraintCoversTerminalHistory(t *testing.T) {
	_, sqlDB := testutil.OpenMigratedPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const insert = `INSERT INTO job_task (instance_id, task_type, status, attempt_no, used_attempts) VALUES ($1, $2, $3, 1, 1)`
	for _, kind := range []model.TaskType{model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypePost} {
		if _, err := sqlDB.ExecContext(ctx, insert, 100, kind, model.TaskSucceeded); err != nil {
			t.Fatal(err)
		}
		_, err := sqlDB.ExecContext(ctx, insert, 100, kind, model.TaskRunning)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "uq_job_task_singleton" {
			t.Fatalf("type=%d duplicate singleton was not rejected by its index: %v", kind, err)
		}
		if _, err := sqlDB.ExecContext(ctx, insert, 101, kind, model.TaskRunning); err != nil {
			t.Fatalf("type=%d independent Instance rejected: %v", kind, err)
		}
	}
	for range 2 {
		if _, err := sqlDB.ExecContext(ctx, insert, 100, model.TaskTypeShard, model.TaskRunning); err != nil {
			t.Fatalf("multiple shard Tasks rejected: %v", err)
		}
	}
}

func TestTaskResultJSONBPreservesAbsenceNullAndLargeNumbers(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, data := range []model.TaskResultData{nil, []byte("null"), []byte(`{"id":9223372036854775807}`), []byte(`[true,"scalar",5]`)} {
		task := model.JobTask{InstanceID: int64(i + 1), TaskType: model.TaskTypeOrdinary, Status: model.TaskSucceeded,
			AttemptNo: 1, UsedAttempts: 1, ResultData: data}
		// Audit fields deliberately use the database defaults rather than an
		// application clock. All execution facts must be supplied explicitly.
		if err := db.WithContext(ctx).Omit("created_at", "updated_at").Create(&task).Error; err != nil {
			t.Fatal(err)
		}
		var stored model.JobTask
		if err := db.WithContext(ctx).First(&stored, task.ID).Error; err != nil {
			t.Fatal(err)
		}
		if (stored.ResultData == nil) != (data == nil) || (data != nil && !json.Valid(stored.ResultData)) {
			t.Fatalf("JSON value changed nullability or validity: input=%s got=%s", data, stored.ResultData)
		}
		if stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() || stored.ExecutorInstanceID != nil || stored.ExecutorAddress != nil {
			t.Fatalf("unexpected initial audit or binding fields: id=%d", stored.ID)
		}
		var isSQLNull bool
		if err := sqlDB.QueryRowContext(ctx, "SELECT result_data IS NULL FROM job_task WHERE id = $1", task.ID).Scan(&isSQLNull); err != nil || isSQLNull != (data == nil) {
			t.Fatalf("SQL NULL distinction lost: id=%d isNull=%v err=%v", task.ID, isSQLNull, err)
		}
		if i == 2 {
			var exact string
			if err := sqlDB.QueryRowContext(ctx, "SELECT result_data->>'id' FROM job_task WHERE id = $1", task.ID).Scan(&exact); err != nil || exact != "9223372036854775807" {
				t.Fatalf("JSON number precision lost: got=%q err=%v", exact, err)
			}
		}
	}
	// Valid JSON syntax does not guarantee PostgreSQL JSONB compatibility.
	// The database error remains available for result persistence to classify.
	_, err := sqlDB.ExecContext(ctx, `INSERT INTO job_task (instance_id, task_type, status, attempt_no, used_attempts, result_data) VALUES (99, 1, 4, 1, 1, $1::jsonb)`, `"\u0000"`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "22P05" {
		t.Fatalf("unsupported JSONB Unicode was not rejected: %v", err)
	}
}
