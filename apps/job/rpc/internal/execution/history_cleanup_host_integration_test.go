//go:build integration

package execution_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func TestServiceContextHistoryCleanupWaitsForLeadershipAndKeepsActiveHistory(t *testing.T) {
	endpoint := dispatchHostEtcd(t)
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	c, applicationName := dispatchHostConfig(t, sqlDB, endpoint)
	instances, err := repository.NewJobInstanceRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	now, err := instances.DatabaseTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	old, created, deadline := now.Add(-8*24*time.Hour), now.Add(-9*24*time.Hour), now.Add(time.Hour)
	var expiredIDs, retainedIDs []int64
	for _, fixture := range []struct {
		status   model.InstanceStatus
		finished *time.Time
		expired  bool
	}{
		{model.InstanceSucceeded, &old, true},
		{model.InstanceFailed, &old, true},
		{model.InstanceCanceled, &old, true},
		{model.InstanceSucceeded, &now, false},
		{model.InstanceRunning, nil, false},
	} {
		requestID, operatorID := uuid.New(), int64(42)
		parent := model.JobInstance{JobID: 11, JobName: "history cleanup host", ExecutorGroupID: 17,
			TriggerType: model.TriggerManual, RequestID: &requestID, TriggeredBy: &operatorID,
			Status: fixture.status, StartedAt: &created, DeadlineAt: &deadline, FinishedAt: fixture.finished,
			CreatedAt: created, UpdatedAt: created,
			ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: "test.history", ExecutionMode: model.ExecutionModeNormal,
				RetryIntervalSeconds: 10, TimeoutSeconds: 864000}}
		if err := db.Create(&parent).Error; err != nil {
			t.Fatal(err)
		}
		taskStatus := model.TaskSucceeded
		if fixture.status == model.InstanceFailed {
			taskStatus = model.TaskFailed
		} else if fixture.status == model.InstanceCanceled {
			taskStatus = model.TaskCanceled
		} else if fixture.status == model.InstanceRunning {
			taskStatus = model.TaskRunning
		}
		executorID, address := uuid.New(), "127.0.0.1:19001"
		task := model.JobTask{InstanceID: parent.ID, TaskType: model.TaskTypeOrdinary, Status: taskStatus,
			AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &created, StartedAt: &created, FinishedAt: fixture.finished,
			ExecutorInstanceID: &executorID, ExecutorAddress: &address, CreatedAt: created, UpdatedAt: created}
		if err := db.Create(&task).Error; err != nil {
			t.Fatal(err)
		}
		if fixture.expired {
			expiredIDs = append(expiredIDs, parent.ID)
		} else {
			retainedIDs = append(retainedIDs, parent.ID)
		}
	}
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
	leader := concurrency.NewElection(session, c.Scheduler.Prefix+"/scheduler/election")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := leader.Campaign(ctx, "test-owned-leader"); err != nil {
		t.Fatal(err)
	}
	host, err := svc.NewServiceContext(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := host.Close(); err != nil {
			t.Error(err)
		}
	})
	dispatchHostEventually(t, "history cleanup host to become a waiting candidate", func(ctx context.Context) bool {
		response, err := client.Get(ctx, c.Scheduler.Prefix+"/scheduler/election/", clientv3.WithPrefix())
		return err == nil && len(response.Kvs) == 2
	})
	// The real constructor has started its process workers, but its elected
	// maintenance callback must remain dormant while this replica is a follower.
	time.Sleep(100 * time.Millisecond)
	assertDispatchHostFollower(t, leader)
	var count int64
	if err := db.Model(&model.JobInstance{}).Where("id IN ?", expiredIDs).Count(&count).Error; err != nil || count != int64(len(expiredIDs)) {
		t.Fatalf("follower deleted terminal history: count=%d error=%v", count, err)
	}
	if err := leader.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	// The default four-hour interval cannot elapse during this assertion. The
	// elected host must run its first pass immediately through real repositories.
	dispatchHostEventually(t, "elected history cleanup to remove expired rounds", func(ctx context.Context) bool {
		var parents, tasks int64
		if db.WithContext(ctx).Model(&model.JobInstance{}).Where("id IN ?", expiredIDs).Count(&parents).Error != nil ||
			db.WithContext(ctx).Model(&model.JobTask{}).Where("instance_id IN ?", expiredIDs).Count(&tasks).Error != nil {
			return false
		}
		return parents == 0 && tasks == 0
	})
	var retained []model.JobInstance
	if err := db.Where("id IN ?", retainedIDs).Order("id").Find(&retained).Error; err != nil {
		t.Fatal(err)
	}
	if len(retained) != 2 || retained[0].Status != model.InstanceSucceeded || retained[0].FinishedAt == nil ||
		!retained[0].FinishedAt.Equal(now) || retained[1].Status != model.InstanceRunning || retained[1].FinishedAt != nil {
		t.Fatalf("cleanup removed recent or active history: %+v", retained)
	}
	if err := db.Model(&model.JobTask{}).Where("instance_id IN ?", retainedIDs).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("cleanup removed retained child Tasks: count=%d error=%v", count, err)
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	if err := host.Drain(drainCtx); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	dispatchHostEventually(t, "history cleanup host database connections to close", func(ctx context.Context) bool {
		var connections int
		err := sqlDB.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1", applicationName).Scan(&connections)
		return err == nil && connections == 0
	})
}
