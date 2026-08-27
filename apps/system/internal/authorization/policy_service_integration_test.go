//go:build integration

package authorization

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gormadapter "github.com/casbin/gorm-adapter/v3"
	"github.com/tokyolab/dogx/apps/system/internal/migration"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/testutil"
	"gorm.io/gorm"
)

type notifierStub struct {
	calls atomic.Int32
	err   error
}

type userSessionRevokerStub struct {
	userIDs []int64
	err     error
}

func (s *userSessionRevokerStub) RevokeAll(_ context.Context, userID int64) error {
	s.userIDs = append(s.userIDs, userID)
	return s.err
}

func (s *notifierStub) Update() error {
	s.calls.Add(1)
	return s.err
}

func TestTransactionBoundAdapterSharesBusinessTransaction(t *testing.T) {
	db := newAuthorizationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rollbackErr := errors.New("force shared transaction rollback")
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txAdapter, err := NewGormAdapter(tx)
		if err != nil {
			return err
		}
		role := model.Role{
			Code:   "transaction_probe",
			Name:   "Transaction Probe",
			Status: model.RecordStatusEnabled,
		}
		if err := tx.Create(&role).Error; err != nil {
			return err
		}
		rule, err := PolicyRule(role.ID, "/transaction-probe", "POST")
		if err != nil {
			return err
		}
		if err := txAdapter.AddPoliciesCtx(ctx, "p", "p", [][]string{rule}); err != nil {
			return err
		}
		return rollbackErr
	})
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("shared transaction error = %v, want %v", err, rollbackErr)
	}

	var count int64
	if err := db.Unscoped().
		Model(&model.Role{}).
		Where("code = ?", "transaction_probe").
		Count(&count).Error; err != nil {
		t.Fatalf("count role after transaction rollback: %v", err)
	}
	if count != 0 {
		t.Fatalf("business write escaped shared transaction: role count = %d", count)
	}
	if err := db.Model(&gormadapter.CasbinRule{}).
		Where("v1 = ?", "/transaction-probe").
		Count(&count).Error; err != nil {
		t.Fatalf("count policy after transaction rollback: %v", err)
	}
	if count != 0 {
		t.Fatalf("policy write escaped shared transaction: policy count = %d", count)
	}
}

func TestRolePolicyServiceReportsTargetDifferenceAndPersistsRequiredAPIs(t *testing.T) {
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
		t.Fatalf("unexpected reported role policy difference: %+v", second)
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

func TestRolePolicyServiceReplacesChangedRoleWithOneDeleteAndOneBatchInsert(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	service, err := NewRolePolicyService(db, &notifierStub{})
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{
		resources["a"].ID,
		resources["b"].ID,
		resources["c"].ID,
		resources["d"].ID,
	}); err != nil {
		t.Fatalf("write initial role policies: %v", err)
	}

	statements := []string{
		`CREATE TABLE casbin_statement_audit (operation text NOT NULL)`,
		`CREATE FUNCTION audit_casbin_statement() RETURNS trigger AS $$
		BEGIN
			INSERT INTO casbin_statement_audit(operation) VALUES (TG_OP);
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql`,
		`CREATE TRIGGER audit_casbin_statement
		AFTER INSERT OR DELETE ON casbin_rule
		FOR EACH STATEMENT EXECUTE FUNCTION audit_casbin_statement()`,
	}
	for _, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatalf("install Casbin statement audit: %v", err)
		}
	}

	result, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["d"].ID})
	if err != nil {
		t.Fatalf("replace role policies: %v", err)
	}
	if result.Added != 0 || result.Removed != 3 {
		t.Fatalf("unexpected reported difference: %+v", result)
	}

	var operations []string
	if err := db.Model(&struct{ Operation string }{}).
		Table("casbin_statement_audit").
		Order("operation ASC").
		Pluck("operation", &operations).Error; err != nil {
		t.Fatalf("load Casbin statement audit: %v", err)
	}
	if len(operations) != 2 || operations[0] != "DELETE" || operations[1] != "INSERT" {
		t.Fatalf("changed role policy used unexpected write statements: %v", operations)
	}

	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["d"].ID}); err != nil {
		t.Fatalf("repeat unchanged role policies: %v", err)
	}
	var count int64
	if err := db.Table("casbin_statement_audit").Count(&count).Error; err != nil {
		t.Fatalf("count Casbin statement audit: %v", err)
	}
	if count != 2 {
		t.Fatalf("unchanged role policy generated writes: statement count = %d", count)
	}
}

func TestRolePolicyServiceListsAssignedAPIIDs(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	service, err := NewRolePolicyService(db, &notifierStub{})
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{
		resources["a"].ID,
		resources["b"].ID,
	}); err != nil {
		t.Fatalf("write role policies: %v", err)
	}

	apiIDs, err := service.ListRoleAPIIDs(ctx, role.ID)
	if err != nil {
		t.Fatalf("list role API ids: %v", err)
	}
	want := []int64{resources["a"].ID, resources["b"].ID, resources["required"].ID}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(apiIDs) != len(want) {
		t.Fatalf("unexpected role API ids: got %v, want %v", apiIDs, want)
	}
	for index := range want {
		if apiIDs[index] != want[index] {
			t.Fatalf("unexpected role API ids: got %v, want %v", apiIDs, want)
		}
	}

	disabled := resources["a"]
	if err := db.Model(&disabled).Update("status", model.RecordStatusDisabled).Error; err != nil {
		t.Fatalf("disable assigned API: %v", err)
	}
	deleted := resources["b"]
	if err := db.Delete(&deleted).Error; err != nil {
		t.Fatalf("soft delete assigned API: %v", err)
	}
	apiIDs, err = service.ListRoleAPIIDs(ctx, role.ID)
	if err != nil {
		t.Fatalf("list role API ids after resource state changes: %v", err)
	}
	want = []int64{resources["a"].ID, resources["required"].ID}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(apiIDs) != len(want) || apiIDs[0] != want[0] || apiIDs[1] != want[1] {
		t.Fatalf("unexpected API ids after resource state changes: got %v, want %v", apiIDs, want)
	}
}

func TestRolePolicyServiceRejectsUnavailableResourcesAndKeepsRequiredAPIs(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	notifier := &notifierStub{}
	service, err := NewRolePolicyService(db, notifier)
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID+1000000, nil); !errors.Is(err, ErrRoleUnavailable) {
		t.Fatalf("missing role error = %v, want %v", err, ErrRoleUnavailable)
	}
	if err := db.Model(&role).Update("status", model.RecordStatusDisabled).Error; err != nil {
		t.Fatalf("disable role: %v", err)
	}
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, nil); !errors.Is(err, ErrRoleUnavailable) {
		t.Fatalf("disabled role error = %v, want %v", err, ErrRoleUnavailable)
	}
	if err := db.Model(&role).Update("status", model.RecordStatusEnabled).Error; err != nil {
		t.Fatalf("enable role: %v", err)
	}

	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["a"].ID + 1000000}); !errors.Is(err, ErrAPIUnavailable) {
		t.Fatalf("missing API error = %v, want %v", err, ErrAPIUnavailable)
	}
	disabledAPI := resources["a"]
	if err := db.Model(&disabledAPI).Update("status", model.RecordStatusDisabled).Error; err != nil {
		t.Fatalf("disable API: %v", err)
	}
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{disabledAPI.ID}); !errors.Is(err, ErrAPIUnavailable) {
		t.Fatalf("disabled API error = %v, want %v", err, ErrAPIUnavailable)
	}

	result, err := service.ReplaceRoleAPIs(ctx, role.ID, nil)
	if err != nil {
		t.Fatalf("persist required-only role policy: %v", err)
	}
	if result.Added != 1 || result.Removed != 0 || notifier.calls.Load() != 1 {
		t.Fatalf("unexpected required-only result: result=%+v notifications=%d", result, notifier.calls.Load())
	}
	rules := loadRoleRules(t, db, role.ID)
	if len(rules) != 1 || rules[0].V1 != "/required" || rules[0].V2 != "POST" {
		t.Fatalf("unexpected required-only role policies: %+v", rules)
	}
}

func TestRolePolicyServiceRollsBackRemovedPoliciesWhenAddFails(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	notifier := &notifierStub{}
	service, err := NewRolePolicyService(db, notifier)
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["a"].ID}); err != nil {
		t.Fatalf("write initial role policy: %v", err)
	}

	if err := db.Exec(`
		CREATE FUNCTION fail_casbin_policy_b_insert() RETURNS trigger AS $$
		BEGIN
			IF NEW.v1 = '/b' THEN
				RAISE EXCEPTION 'forced Casbin policy insert failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql
	`).Error; err != nil {
		t.Fatalf("create failing Casbin insert function: %v", err)
	}
	if err := db.Exec(`
		CREATE TRIGGER fail_casbin_policy_b_insert
		BEFORE INSERT ON casbin_rule
		FOR EACH ROW EXECUTE FUNCTION fail_casbin_policy_b_insert()
	`).Error; err != nil {
		t.Fatalf("create failing Casbin insert trigger: %v", err)
	}

	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["b"].ID}); err == nil {
		t.Fatal("replace role policy succeeded despite forced Casbin insert failure")
	}
	if notifier.calls.Load() != 1 {
		t.Fatalf("failed policy transaction published notification: calls = %d", notifier.calls.Load())
	}

	rules := loadRoleRules(t, db, role.ID)
	paths := make(map[string]bool, len(rules))
	for _, rule := range rules {
		paths[rule.V1] = true
	}
	if len(paths) != 2 || !paths["/a"] || !paths["/required"] || paths["/b"] {
		t.Fatalf("failed policy transaction was not rolled back: %+v", rules)
	}
}

func TestRolePolicyServicePreservesPoliciesWhenRemoveFails(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	notifier := &notifierStub{}
	service, err := NewRolePolicyService(db, notifier)
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["a"].ID}); err != nil {
		t.Fatalf("write initial role policy: %v", err)
	}

	if err := db.Exec(`
		CREATE FUNCTION fail_casbin_policy_a_delete() RETURNS trigger AS $$
		BEGIN
			IF OLD.v1 = '/a' THEN
				RAISE EXCEPTION 'forced Casbin policy delete failure';
			END IF;
			RETURN OLD;
		END;
		$$ LANGUAGE plpgsql
	`).Error; err != nil {
		t.Fatalf("create failing Casbin delete function: %v", err)
	}
	if err := db.Exec(`
		CREATE TRIGGER fail_casbin_policy_a_delete
		BEFORE DELETE ON casbin_rule
		FOR EACH ROW EXECUTE FUNCTION fail_casbin_policy_a_delete()
	`).Error; err != nil {
		t.Fatalf("create failing Casbin delete trigger: %v", err)
	}

	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, nil); err == nil {
		t.Fatal("replace role policy succeeded despite forced Casbin delete failure")
	}
	if notifier.calls.Load() != 1 {
		t.Fatalf("failed policy transaction published notification: calls = %d", notifier.calls.Load())
	}

	rules := loadRoleRules(t, db, role.ID)
	paths := make(map[string]bool, len(rules))
	for _, rule := range rules {
		paths[rule.V1] = true
	}
	if len(paths) != 2 || !paths["/a"] || !paths["/required"] {
		t.Fatalf("failed policy deletion changed persisted policies: %+v", rules)
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

func TestRolePolicyServiceDisablesRoleAndRevokesAssignedUsers(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, _ := seedAuthorizationResources(t, db)
	users := []model.User{
		{Username: "role-status-a", PasswordHash: "hash", Nickname: "A", Status: model.RecordStatusEnabled},
		{Username: "role-status-b", PasswordHash: "hash", Nickname: "B", Status: model.RecordStatusEnabled},
	}
	if err := db.Create(&users).Error; err != nil {
		t.Fatalf("create role users: %v", err)
	}
	for _, user := range users {
		if err := db.Create(&model.UserRole{UserID: user.ID, RoleID: role.ID}).Error; err != nil {
			t.Fatalf("assign role to user %d: %v", user.ID, err)
		}
	}
	service, err := NewRolePolicyService(db, &notifierStub{})
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	revoker := &userSessionRevokerStub{}

	if err := service.UpdateRoleStatus(
		ctx,
		role.ID,
		model.RecordStatusDisabled,
		revoker,
	); err != nil {
		t.Fatalf("disable role: %v", err)
	}
	if len(revoker.userIDs) != 2 || revoker.userIDs[0] != users[0].ID || revoker.userIDs[1] != users[1].ID {
		t.Fatalf("unexpected revoked role users: %v", revoker.userIDs)
	}
	var disabled model.Role
	if err := db.First(&disabled, role.ID).Error; err != nil {
		t.Fatalf("load disabled role: %v", err)
	}
	if disabled.Status != model.RecordStatusDisabled {
		t.Fatalf("role status = %d, want disabled", disabled.Status)
	}

	if err := service.UpdateRoleStatus(ctx, role.ID, model.RecordStatusEnabled, revoker); err != nil {
		t.Fatalf("enable role: %v", err)
	}
	if len(revoker.userIDs) != 2 {
		t.Fatalf("enabling role unexpectedly revoked sessions: %v", revoker.userIDs)
	}
	if err := service.UpdateRoleStatus(ctx, role.ID, model.RecordStatusEnabled, revoker); err != nil {
		t.Fatalf("repeat enabled role status: %v", err)
	}
	if len(revoker.userIDs) != 2 {
		t.Fatalf("idempotent role status update unexpectedly revoked sessions: %v", revoker.userIDs)
	}
}

func TestRolePolicyServiceRollsBackStatusWhenSessionRevocationFails(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, _ := seedAuthorizationResources(t, db)
	user := model.User{
		Username:     "role-status-failure",
		PasswordHash: "hash",
		Nickname:     "Failure",
		Status:       model.RecordStatusEnabled,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create role user: %v", err)
	}
	if err := db.Create(&model.UserRole{UserID: user.ID, RoleID: role.ID}).Error; err != nil {
		t.Fatalf("assign role: %v", err)
	}
	service, err := NewRolePolicyService(db, &notifierStub{})
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	revokeErr := errors.New("redis unavailable")
	if err := service.UpdateRoleStatus(
		ctx,
		role.ID,
		model.RecordStatusDisabled,
		&userSessionRevokerStub{err: revokeErr},
	); !errors.Is(err, revokeErr) {
		t.Fatalf("disable role error = %v, want wrapped %v", err, revokeErr)
	}
	var retained model.Role
	if err := db.First(&retained, role.ID).Error; err != nil {
		t.Fatalf("load role after failed status change: %v", err)
	}
	if retained.Status != model.RecordStatusEnabled {
		t.Fatalf("failed session revocation changed role status: %d", retained.Status)
	}
}

func TestRolePolicyServiceRejectsAssignedRoleAndIgnoresSoftDeletedUsers(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	notifier := &notifierStub{}
	service, err := NewRolePolicyService(db, notifier)
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["a"].ID}); err != nil {
		t.Fatalf("seed role policies: %v", err)
	}
	user := model.User{
		Username:     "role-delete-user",
		PasswordHash: "hash",
		Nickname:     "Delete User",
		Status:       model.RecordStatusEnabled,
	}
	menu := model.Menu{
		AppCode: model.MenuAppAdminWeb,
		Type:    model.MenuTypePage,
		Name:    "Delete Role Menu",
		Path:    "/delete-role-menu",
		Visible: true,
		Status:  model.RecordStatusEnabled,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create role user: %v", err)
	}
	if err := db.Create(&menu).Error; err != nil {
		t.Fatalf("create role menu: %v", err)
	}
	if err := db.Create(&model.UserRole{UserID: user.ID, RoleID: role.ID}).Error; err != nil {
		t.Fatalf("assign role to user: %v", err)
	}
	if err := db.Create(&model.RoleMenu{RoleID: role.ID, MenuID: menu.ID}).Error; err != nil {
		t.Fatalf("assign menu to role: %v", err)
	}
	if _, err := service.DeleteRole(ctx, role.ID); !errors.Is(err, repository.ErrRoleInUse) {
		t.Fatalf("assigned role deletion error = %v, want %v", err, repository.ErrRoleInUse)
	}
	if notifier.calls.Load() != 1 {
		t.Fatalf("rejected deletion published policy notification: %d", notifier.calls.Load())
	}
	var count int64
	if err := db.Model(&model.Role{}).Where("id = ?", role.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("rejected deletion changed role: count=%d error=%v", count, err)
	}
	if err := db.Model(&model.UserRole{}).Where("role_id = ?", role.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("rejected deletion changed user assignments: count=%d error=%v", count, err)
	}
	if err := db.Model(&model.RoleMenu{}).Where("role_id = ?", role.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("rejected deletion changed menu assignments: count=%d error=%v", count, err)
	}
	if rules := loadRoleRules(t, db, role.ID); len(rules) != 2 {
		t.Fatalf("rejected deletion changed role policies: %+v", rules)
	}

	if err := db.Delete(&user).Error; err != nil {
		t.Fatalf("soft delete assigned user: %v", err)
	}
	result, err := service.DeleteRole(ctx, role.ID)
	if err != nil {
		t.Fatalf("delete role referenced only by soft-deleted user: %v", err)
	}
	if result.RemovedPolicies != 2 {
		t.Fatalf("unexpected role deletion result: %+v", result)
	}
	if notifier.calls.Load() != 2 {
		t.Fatalf("unexpected policy notification count: %d", notifier.calls.Load())
	}
	if err := db.Model(&model.Role{}).Where("id = ?", role.ID).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("deleted role remains active: count=%d error=%v", count, err)
	}
	if err := db.Model(&model.UserRole{}).Where("role_id = ?", role.ID).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("deleted role user assignments remain: count=%d error=%v", count, err)
	}
	if err := db.Model(&model.RoleMenu{}).Where("role_id = ?", role.ID).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("deleted role menu assignments remain: count=%d error=%v", count, err)
	}
	if rules := loadRoleRules(t, db, role.ID); len(rules) != 0 {
		t.Fatalf("deleted role policies remain: %+v", rules)
	}
}

func TestRolePolicyServicePreservesRoleWhenPolicyRemovalDuringDeleteFails(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	notifier := &notifierStub{}
	service, err := NewRolePolicyService(db, notifier)
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["a"].ID}); err != nil {
		t.Fatalf("seed role policies: %v", err)
	}

	if err := db.Exec(`
		CREATE FUNCTION fail_deleted_role_policy_delete() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'forced deleted-role policy removal failure';
		END;
		$$ LANGUAGE plpgsql
	`).Error; err != nil {
		t.Fatalf("create failing policy delete function: %v", err)
	}
	if err := db.Exec(`
		CREATE TRIGGER fail_deleted_role_policy_delete
		BEFORE DELETE ON casbin_rule
		FOR EACH ROW EXECUTE FUNCTION fail_deleted_role_policy_delete()
	`).Error; err != nil {
		t.Fatalf("create failing policy delete trigger: %v", err)
	}

	if _, err := service.DeleteRole(ctx, role.ID); err == nil ||
		!strings.Contains(err.Error(), "remove deleted role policies") {
		t.Fatalf("role deletion error = %v, want policy removal failure", err)
	}
	if notifier.calls.Load() != 1 {
		t.Fatalf("failed role deletion published policy notification: %d", notifier.calls.Load())
	}
	var count int64
	if err := db.Model(&model.Role{}).Where("id = ?", role.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("failed policy removal changed role: count=%d error=%v", count, err)
	}
	if rules := loadRoleRules(t, db, role.ID); len(rules) != 2 {
		t.Fatalf("failed policy removal changed role policies: %+v", rules)
	}
}

func TestRolePolicyServiceRollsBackPoliciesAndAssociationsWhenRoleDeleteFails(t *testing.T) {
	db := newAuthorizationDatabase(t)
	role, resources := seedAuthorizationResources(t, db)
	notifier := &notifierStub{}
	service, err := NewRolePolicyService(db, notifier)
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := service.ReplaceRoleAPIs(ctx, role.ID, []int64{resources["a"].ID}); err != nil {
		t.Fatalf("seed role policies: %v", err)
	}

	user := model.User{
		Username:     "role-delete-rollback-user",
		PasswordHash: "hash",
		Nickname:     "Rollback User",
		Status:       model.RecordStatusEnabled,
	}
	menu := model.Menu{
		AppCode: model.MenuAppAdminWeb,
		Type:    model.MenuTypePage,
		Name:    "Rollback Role Menu",
		Path:    "/rollback-role-menu",
		Visible: true,
		Status:  model.RecordStatusEnabled,
	}
	if err := db.Create(&user).Error; err != nil {
		t.Fatalf("create role user: %v", err)
	}
	if err := db.Create(&menu).Error; err != nil {
		t.Fatalf("create role menu: %v", err)
	}
	if err := db.Create(&model.UserRole{UserID: user.ID, RoleID: role.ID}).Error; err != nil {
		t.Fatalf("assign role to user: %v", err)
	}
	if err := db.Create(&model.RoleMenu{RoleID: role.ID, MenuID: menu.ID}).Error; err != nil {
		t.Fatalf("assign menu to role: %v", err)
	}
	if err := db.Delete(&user).Error; err != nil {
		t.Fatalf("soft delete assigned user: %v", err)
	}

	if err := db.Exec(`
		CREATE FUNCTION fail_role_soft_delete() RETURNS trigger AS $$
		BEGIN
			IF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL THEN
				RAISE EXCEPTION 'forced role soft-delete failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql
	`).Error; err != nil {
		t.Fatalf("create failing role delete function: %v", err)
	}
	if err := db.Exec(`
		CREATE TRIGGER fail_role_soft_delete
		BEFORE UPDATE ON sys_role
		FOR EACH ROW EXECUTE FUNCTION fail_role_soft_delete()
	`).Error; err != nil {
		t.Fatalf("create failing role delete trigger: %v", err)
	}

	if _, err := service.DeleteRole(ctx, role.ID); err == nil {
		t.Fatal("role deletion succeeded despite forced soft-delete failure")
	}
	if notifier.calls.Load() != 1 {
		t.Fatalf("failed role deletion published policy notification: %d", notifier.calls.Load())
	}
	var count int64
	if err := db.Model(&model.Role{}).Where("id = ?", role.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("failed deletion changed role: count=%d error=%v", count, err)
	}
	if err := db.Model(&model.UserRole{}).Where("role_id = ?", role.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("failed deletion changed user assignments: count=%d error=%v", count, err)
	}
	if err := db.Model(&model.RoleMenu{}).Where("role_id = ?", role.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("failed deletion changed menu assignments: count=%d error=%v", count, err)
	}
	if rules := loadRoleRules(t, db, role.ID); len(rules) != 2 {
		t.Fatalf("failed deletion changed role policies: %+v", rules)
	}
}

func TestRolePolicyServiceProtectsSystemRoleLifecycle(t *testing.T) {
	db := newAuthorizationDatabase(t)
	service, err := NewRolePolicyService(db, &notifierStub{})
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var systemRole model.Role
	if err := db.Where("code = ?", "super_admin").First(&systemRole).Error; err != nil {
		t.Fatalf("load system role: %v", err)
	}
	revoker := &userSessionRevokerStub{}
	if err := service.UpdateRoleStatus(
		ctx,
		systemRole.ID,
		model.RecordStatusDisabled,
		revoker,
	); !errors.Is(err, repository.ErrSystemRoleProtected) {
		t.Fatalf("disable system role error = %v, want %v", err, repository.ErrSystemRoleProtected)
	}
	if _, err := service.DeleteRole(ctx, systemRole.ID); !errors.Is(err, repository.ErrSystemRoleProtected) {
		t.Fatalf("delete system role error = %v, want %v", err, repository.ErrSystemRoleProtected)
	}
	if len(revoker.userIDs) != 0 {
		t.Fatalf("protected system role revoked sessions: %v", revoker.userIDs)
	}
}

func TestRolePolicyServiceReturnsNotFoundForMissingRoleLifecycle(t *testing.T) {
	db := newAuthorizationDatabase(t)
	service, err := NewRolePolicyService(db, &notifierStub{})
	if err != nil {
		t.Fatalf("create role policy service: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	revoker := &userSessionRevokerStub{}
	const missingRoleID = int64(1 << 62)

	if err := service.UpdateRoleStatus(
		ctx,
		missingRoleID,
		model.RecordStatusDisabled,
		revoker,
	); !errors.Is(err, repository.ErrRoleNotFound) {
		t.Fatalf("missing role status error = %v, want %v", err, repository.ErrRoleNotFound)
	}
	if _, err := service.DeleteRole(ctx, missingRoleID); !errors.Is(err, repository.ErrRoleNotFound) {
		t.Fatalf("missing role deletion error = %v, want %v", err, repository.ErrRoleNotFound)
	}
	if len(revoker.userIDs) != 0 {
		t.Fatalf("missing role lifecycle revoked sessions: %v", revoker.userIDs)
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
