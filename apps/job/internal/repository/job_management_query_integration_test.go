//go:build integration

package repository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestInstanceHistoryFiltersFrozenFactsAndPreservesDeletedReferences(t *testing.T) {
	db, definition := resultRepositoryFixture(t)
	var group model.ExecutorGroup
	if err := db.First(&group, definition.ExecutorGroupID).Error; err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 12, 10, 0, 0, 123456000, time.UTC)
	rows := make([]model.JobInstance, 5)
	for i := range rows {
		requestID, operatorID := uuid.New(), int64(1)
		rows[i] = model.JobInstance{JobID: definition.ID, JobName: "Frozen 50%_!", ExecutorGroupID: group.ID,
			TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID,
			ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ExecutionMode: model.ExecutionModeSharded, Params: strings.Repeat("p", 2048)},
			Status:          model.InstanceSucceeded, CreatedAt: at, UpdatedAt: at}
	}
	plan := at.Add(-time.Hour)
	rows[1].TriggerType, rows[1].ScheduledAt, rows[1].RequestID, rows[1].TriggeredBy, rows[1].Status = model.TriggerScheduled, &plan, nil, nil, model.InstanceFailed
	misfire := plan.Add(-time.Hour)
	rows[2].JobID, rows[2].JobName, rows[2].ExecutorGroupID = definition.ID+1000, "Frozen 50xx!", group.ID+1000
	rows[2].TriggerType, rows[2].ScheduledAt, rows[2].RequestID, rows[2].TriggeredBy = model.TriggerMisfire, &misfire, nil, nil
	rows[2].Status, rows[2].CreatedAt = model.InstanceCanceled, at.Add(time.Hour)
	rows[3].Status, rows[3].CreatedAt = model.InstanceStopping, at.Add(-time.Hour)
	rows[4].JobID, rows[4].JobName, rows[4].Status, rows[4].CreatedAt = definition.ID+2000, "later round", model.InstanceWaiting, at.Add(2*time.Hour)
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(definition).Update("name", "renamed current definition").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(definition).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(&group).Error; err != nil {
		t.Fatal(err)
	}
	// Even an unreadable configuration must not affect the lightweight history.
	if err := db.Model(&model.JobInstance{}).Where("id = ?", rows[0].ID).
		Update("execution_config", gorm.Expr("?::jsonb", `"unusable snapshot"`)).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	to := at.Add(time.Hour)
	for _, tc := range []struct {
		name   string
		filter InstanceFilter
		ids    []int64
		total  int64
	}{
		{"first_page", InstanceFilter{Page: 1, PageSize: 2}, []int64{rows[4].ID, rows[2].ID}, 5},
		{"same_time_second_page", InstanceFilter{Page: 2, PageSize: 2}, []int64{rows[1].ID, rows[0].ID}, 5},
		{"empty_page", InstanceFilter{Page: 4, PageSize: 2}, []int64{}, 5},
		{"identity", InstanceFilter{Page: 1, PageSize: 20, ID: &rows[2].ID}, []int64{rows[2].ID}, 1},
		{"historical_job", InstanceFilter{Page: 1, PageSize: 20, JobID: &definition.ID}, []int64{rows[1].ID, rows[0].ID, rows[3].ID}, 3},
		{"deleted_group", InstanceFilter{Page: 1, PageSize: 20, ExecutorGroupID: &group.ID}, []int64{rows[4].ID, rows[1].ID, rows[0].ID, rows[3].ID}, 4},
		{"literal_name", InstanceFilter{Page: 1, PageSize: 20, JobName: "frozen 50%_!"}, []int64{rows[1].ID, rows[0].ID, rows[3].ID}, 3},
		{"current_name_is_not_history", InstanceFilter{Page: 1, PageSize: 20, JobName: "renamed current"}, []int64{}, 0},
		{"half_open_creation_window", InstanceFilter{Page: 1, PageSize: 20, CreatedFrom: &at, CreatedTo: &to}, []int64{rows[1].ID, rows[0].ID}, 2},
		{"combined", InstanceFilter{Page: 1, PageSize: 20, JobID: &definition.ID, ExecutorGroupID: &group.ID,
			Status: &rows[1].Status, TriggerType: &rows[1].TriggerType, CreatedFrom: &at, CreatedTo: &to}, []int64{rows[1].ID}, 1},
		{"misfire", InstanceFilter{Page: 1, PageSize: 20, TriggerType: &rows[2].TriggerType}, []int64{rows[2].ID}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counter.count, counter.statements = 0, nil
			items, total, err := repo.List(context.Background(), tc.filter)
			if err != nil || total != tc.total || items == nil {
				t.Fatalf("history query: total=%d expected=%d err=%v", total, tc.total, err)
			}
			ids := make([]int64, 0, len(items))
			for _, item := range items {
				ids = append(ids, item.ID)
				if item.ExecutorGroupID == group.ID && (item.ExecutorGroupName == nil || *item.ExecutorGroupName != group.Name) {
					t.Fatal("soft-deleted group name disappeared from history")
				}
				if item.ExecutorGroupID != group.ID && item.ExecutorGroupName != nil {
					t.Fatal("missing group acquired an invented name")
				}
			}
			if !reflect.DeepEqual(ids, tc.ids) || counter.count != 2 {
				t.Fatalf("history page or bounded query count differs: ids=%v expected=%v queries=%d", ids, tc.ids, counter.count)
			}
			for _, forbidden := range []string{"execution_config", "result_summary", "job_definition", "g.token", "FOR UPDATE", "SELECT *"} {
				if strings.Contains(strings.Join(counter.statements, "\n"), forbidden) {
					t.Fatalf("history read unnecessary content: %s", forbidden)
				}
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if items, total, err := repo.List(canceled, InstanceFilter{Page: 1, PageSize: 20}); items != nil || total != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled history query lost its cause: total=%d err=%v", total, err)
	}
}

func TestTaskDetailSelectsOneDocumentWithoutLosingJSONFacts(t *testing.T) {
	db, definition := resultRepositoryFixture(t)
	instance, task := createResultTask(t, db, definition)
	previous := "previous attempt summary"
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).
		Updates(map[string]any{"input_data": gorm.Expr("?::jsonb", `{"id":9223372036854775807}`), "result_data": gorm.Expr("?::jsonb", "null"), "result_summary": previous}).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		selection TaskDataSelection
		want      string
		absent    bool
		forbidden []string
	}{
		{TaskMetadataOnly, "", true, []string{"input_data", "result_data"}},
		{TaskInputData, `{"id": 9223372036854775807}`, false, []string{"result_data"}},
		{TaskResultData, "null", false, []string{"input_data"}},
	} {
		counter.count, counter.statements = 0, nil
		item, err := repo.FindDetail(context.Background(), task.ID, tc.selection)
		if err != nil || item == nil || item.ID != task.ID || item.InstanceID != instance.ID || item.Status != model.TaskRunning ||
			item.ResultSummary == nil || *item.ResultSummary != previous || (item.Data == nil) != tc.absent || (!tc.absent && *item.Data != tc.want) {
			t.Fatalf("selected detail lost persisted facts: selection=%s err=%v", tc.selection, err)
		}
		if counter.count != 1 {
			t.Fatalf("detail added preliminary or per-field queries: %d", counter.count)
		}
		for _, forbidden := range append(tc.forbidden, "job_instance", "job_definition", "FOR UPDATE", "SELECT *") {
			if strings.Contains(counter.statements[0], forbidden) {
				t.Fatalf("detail queried an unrelated field or table: %s", forbidden)
			}
		}
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Update("result_data", nil).Error; err != nil {
		t.Fatal(err)
	}
	item, err := repo.FindDetail(context.Background(), task.ID, TaskResultData)
	if err != nil || item == nil || item.Data != nil {
		t.Fatalf("SQL NULL became a JSON document: err=%v", err)
	}
	if missing, err := repo.FindDetail(context.Background(), task.ID+1000, TaskMetadataOnly); missing != nil || !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("missing detail classification: err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if item, err := repo.FindDetail(ctx, task.ID, TaskInputData); item != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled detail lost its cause: err=%v", err)
	}
}

func TestTaskDetailBoundsExpandedTextWithoutBlockingMetadataOrOtherData(t *testing.T) {
	db, definition := resultRepositoryFixture(t)
	_, task := createResultTask(t, db, definition)
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	// This compact valid input expands past the detail limit when JSONB writes
	// decimal numbers as text. Compressed storage size cannot enforce the limit.
	expanded := "[" + strings.TrimSuffix(strings.Repeat("1e120000,", 80), ",") + "]"
	if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).
		Updates(map[string]any{"input_data": gorm.Expr("?::jsonb", expanded), "result_data": gorm.Expr("?::jsonb", "null")}).Error; err != nil {
		t.Fatal(err)
	}
	if item, err := repo.FindDetail(context.Background(), task.ID, TaskInputData); item != nil || !errors.Is(err, ErrTaskDataTooLarge) {
		t.Fatalf("expanded oversized document was returned: err=%v", err)
	}
	for _, kind := range []TaskDataSelection{TaskMetadataOnly, TaskResultData} {
		if item, err := repo.FindDetail(context.Background(), task.ID, kind); err != nil || item == nil {
			t.Fatalf("oversized unselected document blocked %s: err=%v", kind, err)
		}
	}
	for _, size := range []int{TaskDetailDataLimit, TaskDetailDataLimit + 1} {
		value := `"` + strings.Repeat("x", size-2) + `"`
		if err := db.Model(&model.JobTask{}).Where("id = ?", task.ID).Update("result_data", gorm.Expr("?::jsonb", value)).Error; err != nil {
			t.Fatal(err)
		}
		item, err := repo.FindDetail(context.Background(), task.ID, TaskResultData)
		if size == TaskDetailDataLimit {
			if err != nil || item == nil || item.Data == nil || len(*item.Data) != size {
				t.Fatalf("exact size boundary was rejected or truncated: err=%v", err)
			}
		} else if item != nil || !errors.Is(err, ErrTaskDataTooLarge) {
			t.Fatalf("oversized document was accepted or truncated: err=%v", err)
		}
	}
}

func TestInstanceHistoryPlansUseRealRepositoryQueries(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	if err := db.Exec(`INSERT INTO job_instance
(job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden, execution_config, status, created_at, updated_at)
SELECT n % 400 + 1, 'history round', n % 20 + 1, 1, TIMESTAMPTZ '2026-01-01 UTC' + n * INTERVAL '1 second', FALSE,
'{"schema_version":1,"execution_mode":1}'::jsonb, 3,
TIMESTAMPTZ '2026-01-01 UTC' + n * INTERVAL '1 second', TIMESTAMPTZ '2026-01-01 UTC' + n * INTERVAL '1 second'
FROM generate_series(1, 20000) n`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE job_instance").Error; err != nil {
		t.Fatal(err)
	}
	capture := &executionQueryCapture{queryCounter: &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}}
	repo, err := NewJobInstanceRepository(db.Session(&gorm.Session{Logger: capture}))
	if err != nil {
		t.Fatal(err)
	}
	jobID, groupID := int64(42), int64(7)
	from, to := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 5, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		filter InstanceFilter
		index  string
	}{
		{"global", InstanceFilter{Page: 1, PageSize: 20, CreatedFrom: &from, CreatedTo: &to}, "idx_job_instance_history_created"},
		{"job", InstanceFilter{Page: 1, PageSize: 20, JobID: &jobID, CreatedFrom: &from, CreatedTo: &to}, "idx_job_instance_history_job"},
		{"group", InstanceFilter{Page: 1, PageSize: 20, ExecutorGroupID: &groupID, CreatedFrom: &from, CreatedTo: &to}, "idx_job_instance_history_group"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture.reset()
			items, total, err := repo.List(context.Background(), tc.filter)
			if err != nil || len(items) != 20 || total < 20 || len(capture.queries) != 2 {
				t.Fatalf("history plan workload: rows=%d total=%d queries=%d err=%v", len(items), total, len(capture.queries), err)
			}
			// Observe the planner without settings or hints. Count may choose a
			// sequential scan for a broad range; record rather than forbid it.
			for i, query := range capture.queries {
				plan := explainExecutionQuery(t, db, query)
				t.Logf("history_plan scenario=%s query=%d matching_index=%t sorted=%t", tc.name, i,
					strings.Contains(plan, tc.index), strings.Contains(plan, "Sort"))
			}
		})
	}
}
