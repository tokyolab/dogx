//go:build integration && e2e

package execution_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"gorm.io/gorm"
)

// TestJobShardedStageProcessRecoveryEndToEnd kills a production scheduler after
// its result saver commits, while a test-owned parent lock prevents advancement.
// The replacement must recover from persisted facts without the lost local hint.
// This domain-owned fixture seeds old rounds to exercise the real four-minute
// recovery threshold without changing the production recovery policy.
// Protocol reports stand in for business; the System process tests cover actual
// processors. No executor is registered here, so newly created work is observed
// before business execution, independently of dispatch or result retransmission.
func TestJobShardedStageProcessRecoveryEndToEnd(t *testing.T) {
	for _, stage := range []struct {
		name string
		kind model.TaskType
	}{
		{"saved_preparation_generates_shards", model.TaskTypePrepare},
		{"saved_final_shard_creates_Post", model.TaskTypeShard},
		{"saved_Post_completes_expired_round", model.TaskTypePost},
	} {
		t.Run(stage.name, func(t *testing.T) {
			db, sqlDB := testutil.OpenMigratedPostgres(t)
			endpoint := dispatchHostEtcd(t)
			prefix := "/dogx/test/sharded-recovery-process/" + uuid.NewString()
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			session, err := concurrency.NewSession(client, concurrency.WithTTL(15))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = session.Close() })
			leader := concurrency.NewElection(session, prefix+"/scheduler/election")
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := leader.Campaign(ctx, "test-owned-leader"); err != nil {
				t.Fatal(err)
			}
			group := model.ExecutorGroup{Name: "sharded-recovery-workers", DiscoveryKey: prefix + "/executor",
				Token: "00000000000040008000000000000001"}
			if err := db.Create(&group).Error; err != nil {
				t.Fatal(err)
			}
			var now time.Time
			if err := db.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
				t.Fatal(err)
			}
			deadline := now.Add(10 * time.Minute)
			if stage.kind == model.TaskTypePost {
				deadline = now.Add(-5 * time.Minute)
			}
			address := resultProcessListener(t).Addr().String()
			round := createRunningRecoveryRound(t, db, group, address, stage.name, model.ExecutionModeSharded,
				now.Add(-10*time.Minute), deadline, model.TaskRunning)
			target := shardedRecoveryProcessTarget(t, db, &round, stage.kind, now)
			nodeA := startResultProcess(t, sqlDB, endpoint, prefix, group.Token)
			assertDispatchHostFollower(t, leader)

			locked, err := sqlDB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = locked.Rollback() })
			var id int64
			// Holding this single-parent lock across RPC and process exit is fault
			// injection only. A stage transaction cannot create work, and a normal
			// completion UPDATE cannot commit, before the old process is gone.
			if err := locked.QueryRowContext(ctx, "SELECT id FROM job_instance WHERE id = $1 FOR UPDATE", round.parent.ID).Scan(&id); err != nil {
				t.Fatal(err)
			}
			report := &protocol.ReportTaskResultRequest{
				Auth:  &protocol.GroupAuth{GroupName: group.Name, Token: group.Token},
				JobId: round.parent.JobID, InstanceId: round.parent.ID, TaskId: target.ID, AttemptNo: 1,
				ExecutorInstanceId: target.ExecutorInstanceID.String(), Outcome: protocol.TaskOutcome_SUCCESS,
				Summary: "Stage result persisted before scheduler process exit.",
				Output:  &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte(`{"saved":true}`)},
			}
			if stage.kind == model.TaskTypePrepare {
				report.Output = &protocol.ReportTaskResultRequest_PreparedItems{PreparedItems: &protocol.PreparedItems{
					Items: [][]byte{[]byte(`null`), []byte(`[9007199254740993]`)},
				}}
			}
			if _, err := protocol.NewResultReceiverClient(nodeA.results).ReportTaskResult(ctx, report); err != nil {
				t.Fatalf("submit stage result: %v", err)
			}
			var saved model.JobTask
			dispatchHostEventually(t, "stage result independently committed while advancement is blocked", func(ctx context.Context) bool {
				nodeA.assertAlive(t)
				if err := db.WithContext(ctx).First(&saved, target.ID).Error; err != nil {
					t.Fatal(err)
				}
				return saved.Status == model.TaskSucceeded
			})
			var before []model.JobTask
			if err := db.Where("instance_id = ?", round.parent.ID).Order("id").Find(&before).Error; err != nil {
				t.Fatal(err)
			}
			wantBefore := 2
			if stage.kind == model.TaskTypePrepare {
				wantBefore = 1
			}
			if len(before) != wantBefore || !bytes.Equal(saved.InputData, target.InputData) {
				t.Fatal("blocked advancement created work or result saving changed fixed input")
			}
			if err := nodeA.cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := nodeA.wait(t, 5*time.Second); err == nil {
				t.Fatal("killed scheduler unexpectedly exited cleanly")
			}
			nodeA.waitNoConnections(t, sqlDB)
			if err := locked.Rollback(); err != nil {
				t.Fatal(err)
			}
			nodeB := startResultProcess(t, sqlDB, endpoint, prefix, group.Token)
			if err := leader.Resign(ctx); err != nil {
				t.Fatal(err)
			}
			var parent model.JobInstance
			var tasks []model.JobTask
			// A's abandoned candidacy can retain its lease until natural expiry.
			dispatchHostEventuallyWithin(t, "replacement leader recovered the durable sharded stage", 30*time.Second, func(ctx context.Context) bool {
				nodeB.assertAlive(t)
				if err := db.WithContext(ctx).First(&parent, round.parent.ID).Error; err != nil {
					t.Fatal(err)
				}
				if err := db.WithContext(ctx).Where("instance_id = ?", parent.ID).Order("id").Find(&tasks).Error; err != nil {
					t.Fatal(err)
				}
				if stage.kind == model.TaskTypePost {
					return parent.Status == model.InstanceSucceeded
				}
				return len(tasks) == 3
			})
			if !parent.StartedAt.Equal(*round.parent.StartedAt) || !parent.DeadlineAt.Equal(*round.parent.DeadlineAt) {
				t.Fatal("replacement reset the original round deadline")
			}
			if stage.kind != model.TaskTypePost && parent.Status != model.InstanceRunning {
				t.Fatal("stage creation incorrectly completed the parent")
			}
			if stage.kind == model.TaskTypePost && (len(tasks) != 2 || parent.EndReason == nil ||
				*parent.EndReason != model.EndReasonSuccess || parent.FinishedAt == nil) {
				t.Fatal("completed Post changed the stage count or omitted its conclusion")
			}
			for _, old := range before {
				var current model.JobTask
				if err := db.First(&current, old.ID).Error; err != nil {
					t.Fatal(err)
				}
				if current.Status != old.Status || current.AttemptNo != old.AttemptNo || current.UsedAttempts != old.UsedAttempts ||
					!bytes.Equal(current.InputData, old.InputData) || !bytes.Equal(current.ResultData, old.ResultData) {
					t.Fatal("recovery reran or changed an already-completed stage")
				}
			}
			var createdInputs []string
			for _, task := range tasks {
				if task.ID <= before[len(before)-1].ID {
					continue
				}
				if task.AttemptNo > 1 || task.UsedAttempts > 1 {
					t.Fatal("stage recovery registered multiple attempts")
				}
				if stage.kind == model.TaskTypePrepare && task.TaskType != model.TaskTypeShard {
					t.Fatal("saved preparation created an unexpected stage")
				}
				var compact bytes.Buffer
				if err := json.Compact(&compact, task.InputData); err != nil {
					t.Fatal(err)
				}
				createdInputs = append(createdInputs, compact.String())
				if stage.kind == model.TaskTypeShard {
					var counts struct{ Total, Succeeded, Failed int64 }
					if task.TaskType != model.TaskTypePost || json.Unmarshal(task.InputData, &counts) != nil || counts.Total != 1 || counts.Succeeded != 1 || counts.Failed != 0 {
						t.Fatal("recovered Post lost its fixed statistics")
					}
				}
			}
			if stage.kind == model.TaskTypePrepare && (len(createdInputs) != 2 || createdInputs[0] != "null" || createdInputs[1] != "[9007199254740993]") {
				t.Fatal("recovered shard inputs differ from the committed preparation list")
			}
		})
	}
}

func shardedRecoveryProcessTarget(t *testing.T, db *gorm.DB, round *runningRecoveryRound, kind model.TaskType, now time.Time) model.JobTask {
	t.Helper()
	if kind == model.TaskTypePrepare {
		return round.task
	}
	round.parent.ExecutionConfig.PostEnabled = true
	if err := db.Model(&model.JobInstance{}).Where("id = ?", round.parent.ID).
		Update("execution_config", round.parent.ExecutionConfig).Error; err != nil {
		t.Fatal(err)
	}
	prepared := model.TaskData(`[]`)
	input := model.TaskData(`{"total":0,"succeeded":0,"failed":0}`)
	if kind == model.TaskTypeShard {
		prepared, input = model.TaskData(`[{"work":"fixed"}]`), model.TaskData(`{"work":"fixed"}`)
	}
	if err := db.Model(&model.JobTask{}).Where("id = ?", round.task.ID).Updates(map[string]any{
		"status": model.TaskSucceeded, "result_data": prepared, "finished_at": now, "end_reason": model.EndReasonSuccess,
	}).Error; err != nil {
		t.Fatal(err)
	}
	target := round.task
	target.ID, target.TaskType, target.InputData = 0, kind, input
	if err := db.Create(&target).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&target, target.ID).Error; err != nil {
		t.Fatal(err)
	}
	return target
}
