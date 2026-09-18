//go:build integration

package repository

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"

	"gorm.io/gorm"
)

func TestRoleMenuReplacementIsAtomicScopedAndExplicit(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	repo, err := NewRoleMenuRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	menuRepo, err := NewMenuRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	role := model.Role{Code: "menu_reader", Name: "Reader", Status: 1}
	other := model.Role{Code: "menu_other", Name: "Other", Status: 1}
	if err = db.Create(&role).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	root := model.Menu{Type: 1, Name: "Root", RouteName: "GrantRoot", Path: "/grant-root", Status: 1}
	if err = menuRepo.Create(ctx, &root); err != nil {
		t.Fatal(err)
	}
	page := model.Menu{Type: 2, Name: "Page", RouteName: "GrantPage", Path: "/grant-page", Component: "system/user/index", Status: 1, ParentID: &root.ID}
	if err = menuRepo.Create(ctx, &page); err != nil {
		t.Fatal(err)
	}
	element := model.Menu{Type: 3, Name: "View", Permission: "grant.view", Status: 1, ParentID: &page.ID}
	if err = menuRepo.Create(ctx, &element); err != nil {
		t.Fatal(err)
	}
	mobile := model.Menu{AppCode: model.MenuAppAdminMobile, Type: 1, Name: "Mobile", RouteName: "Mobile", Path: "/mobile", Status: 1}
	if err = db.Create(&mobile).Error; err != nil {
		t.Fatal(err)
	}
	if err = db.Create(&model.RoleMenu{RoleID: role.ID, MenuID: mobile.ID}).Error; err != nil {
		t.Fatal(err)
	}
	assertIDs := func(roles, want []int64) {
		t.Helper()
		got, e := repo.ListMenuIDs(ctx, roles)
		if e != nil || !slices.Equal(got, want) {
			t.Fatalf("ids=%v want=%v err=%v", got, want, e)
		}
	}
	if err = repo.Replace(ctx, role.ID, []int64{element.ID, element.ID}); err != nil {
		t.Fatal(err)
	}
	want := []int64{root.ID, page.ID, element.ID}
	slices.Sort(want)
	assertIDs([]int64{role.ID}, want)
	// A parent ID is an explicit grant, never a wildcard for future children.
	newChild := model.Menu{Type: 3, Name: "Delete", Permission: "grant.delete", Status: 1, ParentID: &page.ID}
	if err = menuRepo.Create(ctx, &newChild); err != nil {
		t.Fatal(err)
	}
	assertIDs([]int64{role.ID}, want)
	if err = repo.Replace(ctx, other.ID, []int64{newChild.ID}); err != nil {
		t.Fatal(err)
	}
	union := append(slices.Clone(want), newChild.ID)
	slices.Sort(union)
	assertIDs([]int64{role.ID, other.ID}, union)
	if err = db.Model(&role).Update("status", 0).Error; err != nil {
		t.Fatal(err)
	}
	assertIDs([]int64{role.ID}, want) // Existing JWT snapshots keep the old role.
	if err = repo.Replace(ctx, role.ID, nil); !errors.Is(err, ErrRoleMenuRoleDisabled) {
		t.Fatal(err)
	}
	if err = db.Model(&role).Update("status", 1).Error; err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []int64{mobile.ID, 999999999} {
		if err = repo.Replace(ctx, role.ID, []int64{invalid}); !errors.Is(err, ErrRoleMenuUnavailable) {
			t.Fatal(err)
		}
		assertIDs([]int64{role.ID}, want)
	}
	forced := errors.New("forced grant insert failure")
	if err = db.Callback().Create().Before("gorm:create").Register("test:role-menu-failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "sys_role_menu" {
			_ = tx.AddError(forced)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Create().Remove("test:role-menu-failure") })
	if err = repo.Replace(ctx, role.ID, want); err != nil {
		t.Fatalf("unchanged set performed a write: %v", err)
	}
	if err = repo.Replace(ctx, role.ID, []int64{newChild.ID}); !errors.Is(err, forced) {
		t.Fatal(err)
	}
	assertIDs([]int64{role.ID}, want)
	if err = db.Callback().Create().Remove("test:role-menu-failure"); err != nil {
		t.Fatal(err)
	}
	if err = repo.Replace(ctx, role.ID, nil); err != nil {
		t.Fatal(err)
	}
	assertIDs([]int64{role.ID}, nil)
	var count int64
	if err = db.Model(&model.RoleMenu{}).Where("role_id = ? AND menu_id = ?", role.ID, mobile.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("mobile grants changed", err)
	}
	var super model.Role
	if err = db.Where("code = ?", model.SuperAdminRoleCode).Take(&super).Error; err != nil {
		t.Fatal(err)
	}
	if err = repo.Replace(ctx, super.ID, []int64{root.ID}); !errors.Is(err, ErrSuperAdminMenusProtected) {
		t.Fatal(err)
	}
	if err = repo.Replace(ctx, 999999999, nil); !errors.Is(err, ErrRoleNotFound) {
		t.Fatal(err)
	}
	if err = menuRepo.Delete(ctx, newChild.ID); err != nil {
		t.Fatal(err)
	}
	if err = repo.Replace(ctx, role.ID, []int64{newChild.ID}); !errors.Is(err, ErrRoleMenuUnavailable) {
		t.Fatal(err)
	}
}
