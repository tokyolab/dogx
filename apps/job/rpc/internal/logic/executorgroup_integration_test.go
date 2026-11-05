//go:build integration

package logic

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"
)

func executorGroupPersistenceFixture(t *testing.T) (*gorm.DB, *svc.ServiceContext) {
	t.Helper()
	db, _ := testutil.OpenMigratedPostgres(t)
	groups, err := repository.NewExecutorGroupRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	transactions, err := repository.NewTransactionRunner(db)
	if err != nil {
		t.Fatal(err)
	}
	return db, &svc.ServiceContext{GroupRepo: groups, Transactions: transactions}
}

func TestExecutorGroupManagementPersistsImmutableIdentityAndCredential(t *testing.T) {
	db, s := executorGroupPersistenceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	created, err := NewCreateExecutorGroupLogic(ctx, s).CreateExecutorGroup(&job.CreateExecutorGroupRequest{Name: "Case-sensitive", DiscoveryKey: "/Group//Executors/"})
	if err != nil || created.GetId() <= 0 {
		t.Fatalf("create group: response=%v error=%v", created, err)
	}
	detail, err := NewGetExecutorGroupLogic(ctx, s).GetExecutorGroup(&job.GetExecutorGroupRequest{Id: created.Id})
	if err != nil || detail.GetGroup().GetName() != "Case-sensitive" || detail.Group.DiscoveryKey != "/Group//Executors/" {
		t.Fatalf("get group: %v", err)
	}
	identity, err := uuid.Parse(detail.Group.Token)
	if err != nil || len(detail.Group.Token) != 32 || identity.Version() != 4 || identity.Variant() != uuid.RFC4122 || strings.Contains(detail.Group.Token, "-") {
		t.Fatal("persisted credential is not a compact UUIDv4")
	}
	for _, stamp := range []string{detail.Group.CreatedAt, detail.Group.UpdatedAt} {
		if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
			t.Fatalf("invalid audit timestamp: %v", err)
		}
	}
	list, err := NewListExecutorGroupsLogic(ctx, s).ListExecutorGroups(&job.ListExecutorGroupsRequest{Page: 1, PageSize: 20, Name: "CASE"})
	if err != nil || list.Total != 1 || len(list.Items) != 1 || list.Items[0].Id != created.Id {
		t.Fatalf("list group: response=%v error=%v", list, err)
	}
	encoded, err := protojson.Marshal(list)
	if err != nil || strings.Contains(string(encoded), detail.Group.Token) || strings.Contains(string(encoded), "token") {
		t.Fatal("management list exposed credentials")
	}
	if _, err := NewDeleteExecutorGroupLogic(ctx, s).DeleteExecutorGroup(&job.DeleteExecutorGroupRequest{Id: created.Id}); err != nil {
		t.Fatalf("delete unused offline group: %v", err)
	}
	var retained model.ExecutorGroup
	if err := db.Unscoped().First(&retained, created.Id).Error; err != nil {
		t.Fatal(err)
	}
	if !retained.DeletedAt.Valid || retained.Token != detail.Group.Token || retained.Name != detail.Group.Name || retained.DiscoveryKey != detail.Group.DiscoveryKey {
		t.Fatal("soft deletion changed retained identity or credential fields")
	}
	_, err = NewGetExecutorGroupLogic(ctx, s).GetExecutorGroup(&job.GetExecutorGroupRequest{Id: created.Id})
	requireSubcode(t, err, subcode.ExecutorGroupNotFound)
	_, err = NewDeleteExecutorGroupLogic(ctx, s).DeleteExecutorGroup(&job.DeleteExecutorGroupRequest{Id: created.Id})
	requireSubcode(t, err, subcode.ExecutorGroupNotFound)
	for _, test := range []struct {
		name, key, want string
	}{
		{"Case-sensitive", "different.rpc", subcode.ExecutorGroupNameExists},
		{"different", "/Group//Executors/", subcode.ExecutorGroupDiscoveryKeyExists},
	} {
		_, err := NewCreateExecutorGroupLogic(ctx, s).CreateExecutorGroup(&job.CreateExecutorGroupRequest{Name: test.name, DiscoveryKey: test.key})
		requireSubcode(t, err, test.want)
	}
	// Exact identities do not normalize case; a differently cased name and key
	// are distinct even after the original identity has been soft deleted.
	if _, err := NewCreateExecutorGroupLogic(ctx, s).CreateExecutorGroup(&job.CreateExecutorGroupRequest{Name: "case-sensitive", DiscoveryKey: "/group//executors/"}); err != nil {
		t.Fatalf("case-sensitive identity creation: %v", err)
	}
}

func TestDeleteExecutorGroupProtectsDisabledJobsAndOldActiveRounds(t *testing.T) {
	db, s := executorGroupPersistenceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := NewCreateExecutorGroupLogic(ctx, s).CreateExecutorGroup(&job.CreateExecutorGroupRequest{Name: "old-group", DiscoveryKey: "old.rpc"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewCreateExecutorGroupLogic(ctx, s).CreateExecutorGroup(&job.CreateExecutorGroupRequest{Name: "new-group", DiscoveryKey: "new.rpc"})
	if err != nil {
		t.Fatal(err)
	}
	definition := integrationValidDefinition()
	definition.ExecutorGroupID, definition.Status = first.Id, basemodel.RecordStatusDisabled
	repositories := integrationRepositories(t, db)
	if _, err := repositories.Jobs.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	_, err = NewDeleteExecutorGroupLogic(ctx, s).DeleteExecutorGroup(&job.DeleteExecutorGroupRequest{Id: first.Id})
	requireSubcode(t, err, subcode.ExecutorGroupInUse)
	instance := integrationSnapshot(definition)
	instance.TriggerType, instance.ScheduledAt = model.TriggerScheduled, &integrationTestNow
	if _, inserted, err := repositories.Instances.Insert(ctx, instance); err != nil || !inserted {
		t.Fatalf("create old round: inserted=%v error=%v", inserted, err)
	}
	// Reconfigure the current Job through its normal disabled-Job update path.
	definition.ExecutorGroupID = second.Id
	if err := s.Transactions(ctx, func(r repository.Repositories) error {
		if err := r.Groups.LockAvailable(ctx, second.Id); err != nil {
			return err
		}
		return r.Jobs.Update(ctx, definition.ID, definition)
	}); err != nil {
		t.Fatal(err)
	}
	for _, state := range []model.InstanceStatus{model.InstanceWaiting, model.InstanceRunning, model.InstanceStopping} {
		if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("status", state).Error; err != nil {
			t.Fatal(err)
		}
		_, err := NewDeleteExecutorGroupLogic(ctx, s).DeleteExecutorGroup(&job.DeleteExecutorGroupRequest{Id: first.Id})
		requireSubcode(t, err, subcode.ExecutorGroupInUse)
	}
	if err := db.Model(&model.JobInstance{}).Where("id = ?", instance.ID).Update("status", model.InstanceCanceled).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := NewDeleteExecutorGroupLogic(ctx, s).DeleteExecutorGroup(&job.DeleteExecutorGroupRequest{Id: first.Id}); err != nil {
		t.Fatalf("terminal old round permanently blocked deletion: %v", err)
	}
	var stored model.JobInstance
	if err := db.First(&stored, instance.ID).Error; err != nil || stored.ExecutorGroupID != first.Id || stored.Status != model.InstanceCanceled {
		t.Fatalf("group deletion changed execution history: %v", err)
	}
}
