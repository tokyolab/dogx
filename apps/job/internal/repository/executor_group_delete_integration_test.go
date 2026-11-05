//go:build integration

package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"gorm.io/gorm"
)

var errGroupReferenceTest = errors.New("group still has references")

func createReferenceTestGroup(t *testing.T, db *gorm.DB, name string) model.ExecutorGroup {
	t.Helper()
	group := model.ExecutorGroup{Name: name, DiscoveryKey: "test." + name + ".rpc", Token: executorGroupTestToken}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	return group
}

func referenceTestJob(groupID int64) *model.JobDefinition {
	return &model.JobDefinition{Name: "reference test", ExecutorGroupID: groupID,
		ProcessorName: "processor", ExecutionMode: model.ExecutionModeNormal,
		CronExpr: "0 * * * * *", Status: basemodel.RecordStatusDisabled,
		RetryIntervalSeconds: 10, TimeoutSeconds: 900}
}

// deleteReferenceTestGroup applies the same repository protocol as management:
// each read follows the group lock in its own READ COMMITTED statement.
func deleteReferenceTestGroup(ctx context.Context, r Repositories, groupID int64) error {
	if err := r.Groups.LockAvailable(ctx, groupID); err != nil {
		return err
	}
	used, err := r.Jobs.HasGroupReferences(ctx, groupID)
	if err != nil {
		return err
	}
	if used {
		return errGroupReferenceTest
	}
	used, err = r.Instances.HasActiveForGroup(ctx, groupID)
	if err != nil {
		return err
	}
	if used {
		return errGroupReferenceTest
	}
	return r.Groups.Delete(ctx, groupID)
}

func TestExecutorGroupReferencesRetainOldRoundsUntilTerminal(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	oldGroup := createReferenceTestGroup(t, db, "old")
	newGroup := createReferenceTestGroup(t, db, "new")
	jobs, _ := NewJobDefinitionRepository(db)
	instances, _ := NewJobInstanceRepository(db)
	groups, _ := NewExecutorGroupRepository(db)
	run, _ := NewTransactionRunner(db)
	ctx := context.Background()
	job := referenceTestJob(oldGroup.ID)
	if _, err := jobs.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	for _, state := range []basemodel.RecordStatus{basemodel.RecordStatusDisabled, basemodel.RecordStatusEnabled} {
		if err := db.Model(job).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
		if found, err := jobs.HasGroupReferences(ctx, oldGroup.ID); err != nil || !found {
			t.Fatalf("Job state %d did not retain group: found=%v error=%v", state, found, err)
		}
		if err := run(ctx, func(r Repositories) error { return deleteReferenceTestGroup(ctx, r, oldGroup.ID) }); !errors.Is(err, errGroupReferenceTest) {
			t.Fatalf("delete referenced group for Job state %d: %v", state, err)
		}
	}
	requestID, operatorID := uuid.New(), int64(1)
	instance := &model.JobInstance{JobID: job.ID, JobName: job.Name, ExecutorGroupID: oldGroup.ID,
		TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1}}
	if _, created, err := instances.Insert(ctx, instance); err != nil || !created {
		t.Fatalf("create frozen round: created=%v error=%v", created, err)
	}
	if err := jobs.Disable(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	job.ExecutorGroupID = newGroup.ID
	if err := run(ctx, func(r Repositories) error {
		if err := r.Groups.LockAvailable(ctx, newGroup.ID); err != nil {
			return err
		}
		return r.Jobs.Update(ctx, job.ID, job)
	}); err != nil {
		t.Fatal(err)
	}
	if found, err := jobs.HasGroupReferences(ctx, oldGroup.ID); err != nil || found {
		t.Fatalf("old Job reference remained after move: found=%v error=%v", found, err)
	}
	// A completed child must not release a STOPPING parent before its own final
	// state is saved. The group lookup needs only the active parent index.
	if err := db.Create(&model.JobTask{InstanceID: instance.ID, TaskType: model.TaskTypeOrdinary,
		Status: model.TaskSucceeded, AttemptNo: 1, UsedAttempts: 1}).Error; err != nil {
		t.Fatal(err)
	}
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceRunning, model.InstanceStopping,
		model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled} {
		if err := db.Model(instance).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
		want := state == model.InstanceWaiting || state == model.InstanceRunning || state == model.InstanceStopping
		if found, err := instances.HasActiveForGroup(ctx, oldGroup.ID); err != nil || found != want {
			t.Fatalf("old-group round state %d: found=%v want=%v error=%v", state, found, want, err)
		}
		if found, err := instances.HasActiveForGroup(ctx, newGroup.ID); err != nil || found {
			t.Fatalf("round followed edited Job group: found=%v error=%v", found, err)
		}
		if want {
			if err := run(ctx, func(r Repositories) error { return deleteReferenceTestGroup(ctx, r, oldGroup.ID) }); !errors.Is(err, errGroupReferenceTest) {
				t.Fatalf("active old-group round did not block delete: %v", err)
			}
		}
	}
	beforeDelete, err := groups.FindByID(ctx, oldGroup.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, func(r Repositories) error { return deleteReferenceTestGroup(ctx, r, oldGroup.ID) }); err != nil {
		t.Fatal(err)
	}
	var stored model.ExecutorGroup
	if err := db.Unscoped().Where("id = ?", oldGroup.ID).Take(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if !stored.DeletedAt.Valid || stored.Name != oldGroup.Name || stored.DiscoveryKey != oldGroup.DiscoveryKey || stored.Token != oldGroup.Token || !stored.UpdatedAt.Equal(beforeDelete.UpdatedAt) {
		t.Fatal("group deletion modified retained identity or ordinary update audit time")
	}
	if err := groups.Delete(ctx, oldGroup.ID); !errors.Is(err, ErrExecutorGroupUnavailable) {
		t.Fatalf("repeat deletion error=%v", err)
	}
	if err := groups.Delete(ctx, oldGroup.ID+1000); !errors.Is(err, ErrExecutorGroupUnavailable) {
		t.Fatalf("missing deletion error=%v", err)
	}
	if err := jobs.Delete(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if found, err := jobs.HasGroupReferences(ctx, newGroup.ID); err != nil || found {
		t.Fatalf("deleted Job still retained group: found=%v error=%v", found, err)
	}
}

func TestExecutorGroupDeleteAndReferenceWritersSerialize(t *testing.T) {
	for _, operation := range []string{"create", "move"} {
		for _, first := range []string{"reference", "delete"} {
			for _, commit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s-first/commit=%v", operation, first, commit), func(t *testing.T) {
					db, _ := testutil.OpenMigratedPostgres(t)
					group := createReferenceTestGroup(t, db, "target")
					other := createReferenceTestGroup(t, db, "source")
					job := referenceTestJob(other.ID)
					if operation == "move" {
						if err := db.Create(job).Error; err != nil {
							t.Fatal(err)
						}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					firstTx := db.WithContext(ctx).Begin()
					if firstTx.Error != nil {
						t.Fatal(firstTx.Error)
					}
					defer firstTx.Rollback()
					groups, _ := NewExecutorGroupRepository(firstTx)
					jobs, _ := NewJobDefinitionRepository(firstTx)
					instances, _ := NewJobInstanceRepository(firstTx)
					firstRepos := Repositories{Groups: groups, Jobs: jobs, Instances: instances}
					var firstPID int
					if err := firstTx.Raw("SELECT pg_backend_pid()").Scan(&firstPID).Error; err != nil {
						t.Fatal(err)
					}
					writeReference := func(r Repositories) error {
						if err := r.Groups.LockAvailable(ctx, group.ID); err != nil {
							return err
						}
						input := *job
						input.ExecutorGroupID = group.ID
						if operation == "move" {
							return r.Jobs.Update(ctx, input.ID, &input)
						}
						_, err := r.Jobs.Create(ctx, &input)
						return err
					}
					firstAction, secondAction := writeReference, func(r Repositories) error { return deleteReferenceTestGroup(ctx, r, group.ID) }
					if first == "delete" {
						firstAction, secondAction = secondAction, firstAction
					}
					if err := firstAction(firstRepos); err != nil {
						t.Fatal(err)
					}
					secondPID := make(chan int, 1)
					finished := make(chan error, 1)
					go func() {
						finished <- db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
							var pid int
							if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
								return err
							}
							secondPID <- pid
							groups, _ := NewExecutorGroupRepository(tx)
							jobs, _ := NewJobDefinitionRepository(tx)
							instances, _ := NewJobInstanceRepository(tx)
							return secondAction(Repositories{Groups: groups, Jobs: jobs, Instances: instances})
						})
					}()
					var pid int
					select {
					case pid = <-secondPID:
					case err := <-finished:
						t.Fatalf("second transaction stopped before its lock: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					waitForGroupRowBlock(t, ctx, db, firstPID, pid)
					var endErr error
					if commit {
						endErr = firstTx.Commit().Error
					} else {
						endErr = firstTx.Rollback().Error
					}
					if endErr != nil {
						t.Fatal(endErr)
					}
					var err error
					select {
					case err = <-finished:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					var want error
					if commit && first == "reference" {
						want = errGroupReferenceTest
					} else if commit {
						want = ErrExecutorGroupUnavailable
					}
					if !errors.Is(err, want) {
						t.Fatalf("second transaction error=%v want=%v", err, want)
					}
					var deleted bool
					if err := db.Raw("SELECT deleted_at IS NOT NULL FROM job_executor_group WHERE id = ?", group.ID).Scan(&deleted).Error; err != nil {
						t.Fatal(err)
					}
					readJobs, _ := NewJobDefinitionRepository(db)
					referenced, err := readJobs.HasGroupReferences(ctx, group.ID)
					if err != nil || deleted == referenced {
						t.Fatalf("group/reference invariant: deleted=%v referenced=%v error=%v", deleted, referenced, err)
					}
				})
			}
		}
	}
}

func waitForGroupRowBlock(t *testing.T, ctx context.Context, db *gorm.DB, blocker, waiter int) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := db.WithContext(ctx).Raw("SELECT ? = ANY(pg_blocking_pids(?))", blocker, waiter).Scan(&blocked).Error; err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("reference writer and delete did not coordinate on the group row")
		case <-ticker.C:
		}
	}
}

func TestExecutorGroupReferenceQueriesUseTargetedIndexes(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	// A synthetic history-heavy data set distinguishes group lookup from a full
	// scan. It is a plan regression, not a measurement of production throughput.
	if err := db.Exec(`INSERT INTO job_instance
		(job_id, job_name, executor_group_id, trigger_type, scheduled_at, params_overridden, execution_config, status)
		SELECT n, 'history', n % 1000 + 1, 1, CURRENT_TIMESTAMP, FALSE, '{}'::jsonb,
		CASE WHEN n % 10 = 0 THEN 6 ELSE 3 END
		FROM generate_series(1, 20000) n`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO job_definition
		(name, remark, executor_group_id, processor_name, execution_mode, post_enabled, params, cron_expr,
		 status, misfire_policy, block_policy, max_retries, retry_interval_seconds, timeout_seconds, deleted_at)
		SELECT 'history', '', n % 1000 + 1, 'processor', 1, FALSE, '', '0 * * * * *',
		 0, 0, 0, 0, 10, 900, CASE WHEN n % 10 = 0 THEN NULL ELSE CURRENT_TIMESTAMP END
		FROM generate_series(1, 20000) n`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE job_instance").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("ANALYZE job_definition").Error; err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ query, index string }{
		{"SELECT EXISTS (SELECT 1 FROM job_instance WHERE executor_group_id = 991 AND status IN (1, 2, 6))", "idx_job_instance_active_group"},
		{"SELECT EXISTS (SELECT 1 FROM job_definition WHERE executor_group_id = 991 AND deleted_at IS NULL)", "idx_job_definition_group_active"},
	} {
		var lines []string
		if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS) " + tc.query).Scan(&lines).Error; err != nil {
			t.Fatal(err)
		}
		plan := strings.Join(lines, "\n")
		if !strings.Contains(plan, tc.index) {
			t.Fatalf("group reference lookup did not use %s:\n%s", tc.index, plan)
		}
		t.Logf("%s:\n%s", tc.index, plan)
	}
}

func TestExecutorGroupReferenceFailuresDoNotAuthorizeDeletion(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	groups, _ := NewExecutorGroupRepository(db)
	jobs, _ := NewJobDefinitionRepository(db)
	instances, _ := NewJobInstanceRepository(db)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if found, err := jobs.HasGroupReferences(ctx, 1); found || !errors.Is(err, context.Canceled) {
		t.Fatalf("Job reference error=%v found=%v", err, found)
	}
	if found, err := instances.HasActiveForGroup(ctx, 1); found || !errors.Is(err, context.Canceled) {
		t.Fatalf("Instance reference error=%v found=%v", err, found)
	}
	if err := groups.Delete(ctx, 1); !errors.Is(err, context.Canceled) || errors.Is(err, ErrExecutorGroupUnavailable) {
		t.Fatalf("delete dependency failure became absence: %v", err)
	}
}
