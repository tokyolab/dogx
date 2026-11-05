//go:build integration

package repository

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const executorGroupTestToken = "00000000000040008000000000000001"

func TestExecutorGroupRepositoryReadsAndSoftDeletion(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	repo, err := NewExecutorGroupRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	group := model.ExecutorGroup{
		Base: basemodel.Base{ID: 900, CreatedAt: old, UpdatedAt: old, DeletedAt: gorm.DeletedAt{Time: old, Valid: true}},
		Name: "Alpha%_!", DiscoveryKey: "test.alpha.rpc", Token: executorGroupTestToken,
	}
	id, err := repo.Create(ctx, &group)
	if err != nil || id != group.ID || id <= 0 || id == 900 || group.DeletedAt.Valid || !group.CreatedAt.After(old) || !group.UpdatedAt.After(old) {
		t.Fatalf("creation did not replace caller Base: id = %d, error = %v", id, err)
	}
	other := model.ExecutorGroup{Name: "Alpha other", DiscoveryKey: "test.other.rpc", Token: executorGroupTestToken}
	if _, err := repo.Create(ctx, &other); err != nil {
		t.Fatal(err)
	}
	detail, err := repo.FindByID(ctx, id)
	if err != nil || detail == nil || detail.Name != group.Name || detail.DiscoveryKey != group.DiscoveryKey || detail.Token != group.Token {
		t.Fatalf("group detail did not preserve execution fields: %v", err)
	}
	byID, err := repo.FindExecutionGroupByID(ctx, id)
	if err != nil || byID == nil || byID.ID != id || byID.Name != group.Name || byID.DiscoveryKey != group.DiscoveryKey || byID.Token != group.Token {
		t.Fatalf("group configuration by ID mismatch: %v", err)
	}
	byName, err := repo.FindExecutionGroupByName(ctx, group.Name)
	if err != nil || byName == nil || *byName != *byID {
		t.Fatalf("group configuration by name mismatch: %v", err)
	}
	if value, err := repo.FindExecutionGroupByName(ctx, strings.ToLower(group.Name)); value != nil || !errors.Is(err, ErrExecutorGroupUnavailable) {
		t.Fatalf("group name lookup was not case-sensitive: %v", err)
	}

	counter := &queryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	readRepo, err := NewExecutorGroupRepository(db.Session(&gorm.Session{Logger: counter}))
	if err != nil {
		t.Fatal(err)
	}
	items, total, err := readRepo.List(ctx, ExecutorGroupFilter{Page: 1, PageSize: 20, Name: "a%_!"})
	if err != nil || total != 1 || len(items) != 1 || items[0].ID != id || counter.count != 2 {
		t.Fatalf("literal list: rows = %d, total = %d, queries = %d, error = %v", len(items), total, counter.count, err)
	}
	for _, statement := range counter.statements {
		if strings.Contains(strings.ToLower(statement), "token") || strings.Contains(statement, executorGroupTestToken) {
			t.Fatal("management list selected a credential")
		}
	}
	items, total, err = repo.List(ctx, ExecutorGroupFilter{Page: 1, PageSize: 1})
	if err != nil || total != 2 || len(items) != 1 || items[0].ID != other.ID {
		t.Fatalf("descending pagination: rows = %d, total = %d, error = %v", len(items), total, err)
	}
	items, total, err = repo.List(ctx, ExecutorGroupFilter{Page: 3, PageSize: 1})
	if err != nil || items == nil || len(items) != 0 || total != 2 {
		t.Fatalf("empty page: rows = %d, total = %d, error = %v", len(items), total, err)
	}
	configs, err := repo.LoadExecutionGroups(ctx)
	if err != nil || len(configs) != 2 {
		t.Fatalf("full snapshot: rows = %d, error = %v", len(configs), err)
	}
	// Delete is fixture setup, not the management operation: that operation must
	// first coordinate references under the group lock in its own transaction.
	if err := db.Delete(&group).Error; err != nil {
		t.Fatal(err)
	}
	var stored model.ExecutorGroup
	if err := db.Unscoped().Where("id = ?", id).Take(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if !stored.DeletedAt.Valid || stored.Name != group.Name || stored.DiscoveryKey != group.DiscoveryKey || stored.Token != group.Token || !stored.UpdatedAt.Equal(detail.UpdatedAt) {
		t.Fatal("soft deletion changed immutable credentials or the audit update timestamp")
	}
	if got, err := repo.FindByID(ctx, id); got != nil || !errors.Is(err, ErrExecutorGroupUnavailable) {
		t.Fatalf("deleted group detail: %v", err)
	}
	if got, err := repo.FindExecutionGroupByID(ctx, id); got != nil || !errors.Is(err, ErrExecutorGroupUnavailable) {
		t.Fatalf("deleted group configuration: %v", err)
	}
	if got, err := repo.FindExecutionGroupByName(ctx, group.Name); got != nil || !errors.Is(err, ErrExecutorGroupUnavailable) {
		t.Fatalf("deleted group name: %v", err)
	}
	configs, err = repo.LoadExecutionGroups(ctx)
	if err != nil || len(configs) != 1 || configs[0].ID != other.ID {
		t.Fatalf("snapshot retained deleted group: rows = %d, error = %v", len(configs), err)
	}
	if err := db.Delete(&other).Error; err != nil {
		t.Fatal(err)
	}
	configs, err = repo.LoadExecutionGroups(ctx)
	if err != nil || configs == nil || len(configs) != 0 {
		t.Fatalf("empty snapshot: rows = %d, error = %v", len(configs), err)
	}
}

func TestExecutorGroupRepositoryFullTableIdentityConstraints(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	repo, err := NewExecutorGroupRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	group := model.ExecutorGroup{Name: "reserved", DiscoveryKey: "test.reserved.rpc", Token: executorGroupTestToken}
	if _, err := repo.Create(ctx, &group); err != nil {
		t.Fatal(err)
	}
	for _, deleted := range []bool{false, true} {
		if deleted {
			if err := db.Delete(&group).Error; err != nil {
				t.Fatal(err)
			}
		}
		for _, tc := range []struct {
			name string
			key  string
			want error
		}{
			{group.Name, "test.another.rpc", ErrExecutorGroupNameExists},
			{"another", group.DiscoveryKey, ErrExecutorGroupDiscoveryKeyExists},
		} {
			copy := model.ExecutorGroup{Name: tc.name, DiscoveryKey: tc.key, Token: executorGroupTestToken}
			if id, err := repo.Create(ctx, &copy); id != 0 || !errors.Is(err, tc.want) {
				t.Fatalf("identity constraint after deletion = %t: id = %d, error = %v", deleted, id, err)
			}
		}
	}
	// Identity matching remains exact: case-distinct names and keys are valid.
	upper := model.ExecutorGroup{Name: "Reserved", DiscoveryKey: "test.Reserved.rpc", Token: executorGroupTestToken}
	if _, err := repo.Create(ctx, &upper); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"name", "discovery_key", "token"} {
		values := map[string]any{"name": "null-test", "discovery_key": "test.null.rpc", "token": executorGroupTestToken}
		values[column] = nil
		err := db.Model(&model.ExecutorGroup{}).Create(values).Error
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23502" || pgErr.ColumnName != column {
			t.Fatalf("column %s did not reject SQL NULL with its own constraint", column)
		}
	}
}

func TestExecutorGroupRepositoryDatabaseFailuresAreNotAbsence(t *testing.T) {
	db, _ := testutil.OpenMigratedPostgres(t)
	repo, err := NewExecutorGroupRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if groups, err := repo.LoadExecutionGroups(ctx); groups != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("full load discarded dependency failure: %v", err)
	}
	if group, err := repo.FindExecutionGroupByID(ctx, 1); group != nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrExecutorGroupUnavailable) {
		t.Fatalf("ID lookup discarded dependency failure: %v", err)
	}
	if group, err := repo.FindExecutionGroupByName(ctx, "missing"); group != nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrExecutorGroupUnavailable) {
		t.Fatalf("name lookup discarded dependency failure: %v", err)
	}
}
