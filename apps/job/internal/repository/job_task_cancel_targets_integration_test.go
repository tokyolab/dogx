//go:build integration

package repository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestCancelTargetQueryPaginatesDistinctAddressesAcrossAllTaskStates(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, _ := createResultTask(t, db, job)
	other, _ := createResultTask(t, db, job)
	addresses := []string{"127.0.0.1:9000", "[::1]:9000", "10.0.0.1:9000", "[2001:db8::1]:9000", "192.0.2.1:9000", "172.16.0.1:9000"}
	var tasks []model.JobTask
	for index, address := range addresses {
		for range 3 {
			tasks = append(tasks, model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeShard,
				Status: model.TaskStatus(index + 1), ExecutorAddress: &address})
		}
	}
	empty, excluded := "", "203.0.113.1:9000"
	tasks = append(tasks,
		model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeShard, Status: model.TaskCanceled, ExecutorAddress: &empty},
		model.JobTask{InstanceID: other.ID, TaskType: model.TaskTypeShard, Status: model.TaskRunning, ExecutorAddress: &excluded})
	if err := db.Create(&tasks).Error; err != nil {
		t.Fatal(err)
	}
	// Database completion does not prove that the remote business exited, so it
	// must not remove addresses from this independent best-effort notification.
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("status", model.InstanceCanceled).Error; err != nil {
		t.Fatal(err)
	}
	var expected []string
	if err := db.Raw(`SELECT DISTINCT executor_address FROM job_task
		WHERE instance_id = ? AND executor_address <> '' ORDER BY executor_address`, instance.ID).Scan(&expected).Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	var actual []string
	cursor := ""
	for {
		counter.count, counter.statements = 0, nil
		page, err := repo.ListCancelTargets(context.Background(), instance.ID, cursor, 2)
		if err != nil || page == nil || len(page) > 2 || counter.count != 1 {
			t.Fatalf("bounded address query: page=%v count=%d err=%v", page, counter.count, err)
		}
		statement := counter.statements[0]
		for _, forbidden := range []string{"DISTINCT", "OFFSET", "FOR UPDATE", "result_data", "status"} {
			if strings.Contains(statement, forbidden) {
				t.Fatalf("address query added forbidden work %q: %s", forbidden, statement)
			}
		}
		if len(page) == 0 {
			break
		}
		actual = append(actual, page...)
		if len(actual) > len(expected) {
			t.Fatalf("address pagination repeated targets: %v", actual)
		}
		cursor = page[len(page)-1]
	}
	if !reflect.DeepEqual(actual, expected) || len(actual) != len(addresses) {
		t.Fatalf("address traversal lost states, order or distinctness: got=%v want=%v", actual, expected)
	}
	missing, err := repo.ListCancelTargets(context.Background(), 9223372036854775807, "", 32)
	if err != nil || missing == nil || len(missing) != 0 {
		t.Fatalf("missing Instance addresses: %v, %v", missing, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repo.ListCancelTargets(ctx, instance.ID, "", 32); !errors.Is(err, context.Canceled) {
		t.Fatalf("address query lost cancellation cause: %v", err)
	}
}

func TestCancelTargetPaginationAcceptsLaterBindingsWithoutRewinding(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, _ := createResultTask(t, db, job)
	insert := func(addresses ...string) {
		t.Helper()
		var tasks []model.JobTask
		for _, address := range addresses {
			tasks = append(tasks, model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeShard,
				Status: model.TaskRunning, ExecutorAddress: &address})
		}
		if err := db.Create(&tasks).Error; err != nil {
			t.Fatal(err)
		}
	}
	insert("127.0.0.1:9000", "127.0.0.3:9000")
	repo, err := NewJobTaskRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	first, err := repo.ListCancelTargets(context.Background(), instance.ID, "", 1)
	if err != nil || !reflect.DeepEqual(first, []string{"127.0.0.1:9000"}) {
		t.Fatalf("first address: %v %v", first, err)
	}
	insert("127.0.0.0:9000", "127.0.0.2:9000", "127.0.0.1:9000")
	second, err := repo.ListCancelTargets(context.Background(), instance.ID, first[0], 32)
	if err != nil || !reflect.DeepEqual(second, []string{"127.0.0.2:9000", "127.0.0.3:9000"}) {
		t.Fatalf("later bindings rewound or escaped keyset pagination: %v %v", second, err)
	}
}

func TestCancelTargetQuerySeeksPastHeavyDuplicateAddresses(t *testing.T) {
	db, job := resultRepositoryFixture(t)
	instance, _ := createResultTask(t, db, job)
	// Many Tasks share the first address. DISTINCT/LIMIT would visit those
	// duplicate index entries before reaching the next distinct address.
	if err := db.Exec(`INSERT INTO job_task
		(instance_id, task_type, status, attempt_no, used_attempts, executor_address)
		SELECT ?, 3, 6, 1, 1, '10.0.0.0:9000' FROM generate_series(1, 30000)`, instance.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO job_task
		(instance_id, task_type, status, attempt_no, used_attempts, executor_address)
		SELECT ?, 3, 4, 1, 1, '10.0.0.' || address.n || ':9000'
		FROM generate_series(1, 64) AS address(n) CROSS JOIN generate_series(1, 16)`, instance.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE job_task").Error; err != nil {
		t.Fatal(err)
	}
	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	capture := &executionQueryCapture{queryCounter: counter}
	// GORM Scan temporarily replaces the logger, bypassing ParamsFilter. Capture
	// its row callback inputs on this test's DB before GORM clears the statement.
	const callbackName = "test:capture_cancel_target_query"
	if err := db.Callback().Row().After("gorm:row").Register(callbackName, func(tx *gorm.DB) {
		capture.queries = append(capture.queries, executionQuery{
			sql: tx.Statement.SQL.String(), vars: append([]any(nil), tx.Statement.Vars...),
		})
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Callback().Row().Remove(callbackName); err != nil {
			t.Error(err)
		}
	})
	repo, err := NewJobTaskRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	cursor := ""
	for range 2 {
		capture.reset()
		page, err := repo.ListCancelTargets(context.Background(), instance.ID, cursor, 32)
		if err != nil || len(page) != 32 || counter.count != 1 || len(capture.queries) != 1 {
			t.Fatalf("duplicate-heavy address page: count=%d page=%v err=%v", counter.count, page, err)
		}
		plan := explainExecutionQuery(t, db, capture.queries[0])
		if strings.Count(plan, "idx_job_task_instance_address") < 2 || !strings.Contains(plan, "Recursive Union") ||
			strings.Contains(plan, "Seq Scan") || strings.Contains(plan, "Unique") || strings.Contains(plan, "HashAggregate") ||
			!strings.Contains(plan, "executor_address >") {
			t.Fatalf("address traversal did not seek each next distinct value using its index:\n%s", plan)
		}
		cursor = page[len(page)-1]
	}
}
