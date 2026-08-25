//go:build integration

package authorization

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gormadapter "github.com/casbin/gorm-adapter/v3"
	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
	"gorm.io/gorm"
)

type notifierStub struct {
	calls atomic.Int32
	err   error
}

func (s *notifierStub) Update() error {
	s.calls.Add(1)
	return s.err
}

func TestRolePolicyServicePersistsOnlyTargetDifferenceAndRequiredAPIs(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	notifier := &notifierStub{}
	service, err := NewRolePolicyService(db, notifier)
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{
		resources["a"].ID,
		resources["b"].ID,
		resources["c"].ID,
	})
	if err != nil {
		t.Fatalf("write initial role policy: %v", err)
	}
	if first.Added != 4 || first.Removed != 0 {
		t.Fatalf("required API was not included in initial diff: %+v", first)
	}

	second, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{
		resources["b"].ID,
		resources["c"].ID,
		resources["d"].ID,
	})
	if err != nil {
		t.Fatalf("replace role policy: %v", err)
	}
	if second.Added != 1 || second.Removed != 1 {
		t.Fatalf("role policy was not persisted as a difference: %+v", second)
	}
	if notifier.calls.Load() != 2 {
		t.Fatalf("unexpected policy notification count: %d", notifier.calls.Load())
	}

	rules := loadRoleRules(t, db, role.ID)
	paths := make(map[string]bool, len(rules))
	for _, rule := range rules {
		paths[rule.V1] = true
	}
	if len(paths) != 4 || paths["/a"] || !paths["/b"] || !paths["/c"] || !paths["/d"] || !paths["/required"] {
		t.Fatalf("unexpected persisted role policies: %+v", rules)
	}

	unchanged, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{
		resources["b"].ID,
		resources["c"].ID,
		resources["d"].ID,
	})
	if err != nil {
		t.Fatalf("write unchanged role policy: %v", err)
	}
	if unchanged.Changed() || notifier.calls.Load() != 2 {
		t.Fatalf("unchanged policy generated writes or notification: result=%+v notifications=%d", unchanged, notifier.calls.Load())
	}
}

func TestRolePolicyServiceSerializesConcurrentUpdatesForSameRole(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	service, err := NewRolePolicyService(db, &notifierStub{})
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	targets := [][]int64{
		{resources["a"].ID, resources["b"].ID},
		{resources["c"].ID, resources["d"].ID},
	}
	start := make(chan struct{})
	errorsByWriter := make(chan error, len(targets))
	var wait sync.WaitGroup
	for _, target := range targets {
		target := target
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := service.ReplaceRoleAPIs(ctx, role.ID, target)
			errorsByWriter <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsByWriter)
	for err := range errorsByWriter {
		if err != nil {
			t.Fatalf("concurrent role policy update: %v", err)
		}
	}

	rules := loadRoleRules(t, db, role.ID)
	paths := make(map[string]bool, len(rules))
	for _, rule := range rules {
		paths[rule.V1] = true
	}
	firstComplete := paths["/a"] && paths["/b"] && !paths["/c"] && !paths["/d"]
	secondComplete := !paths["/a"] && !paths["/b"] && paths["/c"] && paths["/d"]
	if len(paths) != 3 || (!firstComplete && !secondComplete) || !paths["/required"] {
		t.Fatalf("concurrent updates left a partial target set: %+v", rules)
	}
}

func newAuthorizationDatabase(t testing.TB) *gorm.DB {
	t.Helper()
	db, sqlDB := testutil.OpenPostgres(t)
	provider, err := migration.NewProvider(sqlDB)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return db
}

func seedAuthorizationResources(t testing.TB, db *gorm.DB) (model.Role, map[string]model.API) {
	t.Helper()
	role := model.Role{Code: "operator", Name: "Operator", Status: model.RecordStatusEnabled}
	if err := db.Create(&role).Error; err != nil {
		t.Fatalf("create role: %v", err)
	}
	resources := map[string]model.API{
		"a":        {ServiceName: "system-api", Group: "test", Name: "A", Path: "/a", Method: "POST", Status: model.RecordStatusEnabled},
		"b":        {ServiceName: "system-api", Group: "test", Name: "B", Path: "/b", Method: "POST", Status: model.RecordStatusEnabled},
		"c":        {ServiceName: "system-api", Group: "test", Name: "C", Path: "/c", Method: "POST", Status: model.RecordStatusEnabled},
		"d":        {ServiceName: "system-api", Group: "test", Name: "D", Path: "/d", Method: "POST", Status: model.RecordStatusEnabled},
		"required": {ServiceName: "system-api", Group: "test", Name: "Required", Path: "/required", Method: "POST", IsRequired: true, Status: model.RecordStatusEnabled},
	}
	for name, resource := range resources {
		resource := resource
		if err := db.Create(&resource).Error; err != nil {
			t.Fatalf("create API %s: %v", name, err)
		}
		resources[name] = resource
	}
	return role, resources
}

func loadRoleRules(t testing.TB, db *gorm.DB, roleID int64) []gormadapter.CasbinRule {
	t.Helper()
	subject, err := RoleSubject(roleID)
	if err != nil {
		t.Fatalf("build role subject: %v", err)
	}
	var rules []gormadapter.CasbinRule
	if err := db.Where("ptype = ? AND v0 = ?", "p", subject).Order("v1, v2").Find(&rules).Error; err != nil {
		t.Fatalf("load persisted Casbin rules: %v", err)
	}
	return rules
}
