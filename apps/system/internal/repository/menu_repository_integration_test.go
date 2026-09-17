//go:build integration

package repository

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

func TestMenuDeletionRollsBackGrantCleanupOnFailure(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	repo, err := NewMenuRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	menu := &model.Menu{Type: model.MenuTypePage, Name: "Rollback", RouteName: "Rollback", Path: "/rollback", Component: "rollback/index"}
	if err := repo.Create(ctx, menu); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&model.RoleMenu{RoleID: 999, MenuID: menu.ID}).Error; err != nil {
		t.Fatal(err)
	}
	forced := errors.New("forced menu deletion failure")
	if err := db.Callback().Delete().Before("gorm:delete").Register("test:menu-delete-failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "sys_menu" {
			_ = tx.AddError(forced)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Delete().Remove("test:menu-delete-failure") })
	if err := repo.Delete(ctx, menu.ID); !errors.Is(err, forced) {
		t.Fatalf("expected deletion failure: %v", err)
	}
	if _, err := repo.FindByID(ctx, menu.ID); err != nil {
		t.Fatalf("menu deletion escaped rollback: %v", err)
	}
	var count int64
	if err := db.Model(&model.RoleMenu{}).Where("menu_id = ?", menu.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("grant cleanup escaped rollback: count=%d err=%v", count, err)
	}
}

func TestMenuRepositoryLifecycle(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	repo, err := NewMenuRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	root := &model.Menu{Type: model.MenuTypeDirectory, Name: "Root", RouteName: "TestRoot", Path: "/menu-test", Status: 1, Visible: true}
	if err = repo.Create(ctx, root); err != nil {
		t.Fatal(err)
	}
	leaf := &model.Menu{ParentID: &root.ID, Type: model.MenuTypePage, Name: "Leaf", RouteName: "TestLeaf", Path: "/menu-test/leaf", Component: "system/leaf/index", Sort: 10, Status: 1, Visible: true, Remark: "original"}
	if err = repo.Create(ctx, leaf); err != nil {
		t.Fatal(err)
	}
	duplicate := *leaf
	duplicate.ID = 0
	if err = repo.Create(ctx, &duplicate); !errors.Is(err, ErrMenuRouteNameExists) {
		t.Fatalf("duplicate=%v", err)
	}
	duplicate.RouteName = "AnotherLeaf"
	if err = repo.Create(ctx, &duplicate); !errors.Is(err, ErrMenuPathExists) {
		t.Fatalf("path duplicate=%v", err)
	}
	if err = repo.Delete(ctx, root.ID); !errors.Is(err, ErrMenuHasChildren) {
		t.Fatalf("parent delete=%v", err)
	}
	converted := &model.Menu{ParentID: &root.ID, Type: model.MenuTypeElement, Name: "Create", Permission: "test.create", Visible: false, Sort: 0}
	if err = repo.Update(ctx, leaf.ID, converted); err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindByID(ctx, leaf.ID)
	if err != nil || got.Status != 1 || got.Visible || got.Sort != 0 || got.Remark != "" || got.Path != "" || got.Component != "" {
		t.Fatalf("zero values: %+v err=%v", got, err)
	}
	if err = repo.UpdateStatus(ctx, leaf.ID, 0); err != nil {
		t.Fatal(err)
	}
	got, err = repo.FindByID(ctx, leaf.ID)
	if err != nil || got.Status != 0 {
		t.Fatal(err)
	}
	// A type change keeps the existing grant; deleting the leaf clears it.
	grant := model.RoleMenu{RoleID: 999, MenuID: leaf.ID}
	if err = db.Create(&grant).Error; err != nil {
		t.Fatal(err)
	}
	converted.Type = model.MenuTypePage
	converted.Permission = ""
	converted.RouteName = "BackToPage"
	converted.Path = "/back"
	converted.Component = "system/back/index"
	if err = repo.Update(ctx, leaf.ID, converted); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err = db.Model(&model.RoleMenu{}).Where("menu_id = ?", leaf.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("type change removed grant", err)
	}
	if err = repo.Delete(ctx, leaf.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.FindByID(ctx, leaf.ID); !errors.Is(err, ErrMenuNotFound) {
		t.Fatal(err)
	}
	if err = db.Model(&model.RoleMenu{}).Where("menu_id = ?", leaf.ID).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("grant remains", err)
	}
	if err = repo.Delete(ctx, root.ID); err != nil {
		t.Fatal(err)
	}
}
func TestMenuUpdateUniqueConflictsLeaveBothRecordsUnchanged(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	repo, err := NewMenuRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	oldParent := &model.Menu{Type: model.MenuTypeDirectory, Name: "Old parent", RouteName: "OldParent", Path: "/old-parent"}
	newParent := &model.Menu{Type: model.MenuTypeDirectory, Name: "New parent", RouteName: "NewParent", Path: "/new-parent"}
	for _, parent := range []*model.Menu{oldParent, newParent} {
		if err := repo.Create(ctx, parent); err != nil {
			t.Fatal(err)
		}
	}
	source := &model.Menu{
		ParentID: &oldParent.ID, Type: model.MenuTypePage, Name: "Original", RouteName: "Original",
		Path: "/original", Component: "original/index", Sort: 10, Status: model.RecordStatusEnabled,
		Visible: true, KeepAlive: true, Remark: "original remark",
	}
	// Disabled menus still reserve their route names and paths.
	occupied := &model.Menu{
		ParentID: &newParent.ID, Type: model.MenuTypePage, Name: "Occupied", RouteName: "Occupied",
		Path: "/occupied", Component: "occupied/index", Status: model.RecordStatusDisabled,
	}
	for _, menu := range []*model.Menu{source, occupied} {
		if err := repo.Create(ctx, menu); err != nil {
			t.Fatal(err)
		}
	}
	snapshots := make([]*model.Menu, 0, 2)
	for _, id := range []int64{source.ID, occupied.ID} {
		snapshot, err := repo.FindByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snapshot)
	}
	for _, tc := range []struct {
		name      string
		routeName string
		path      string
		wantErr   error
	}{
		{"case-insensitive route name", strings.ToLower(occupied.RouteName), "/moved", ErrMenuRouteNameExists},
		{"path", "Moved", occupied.Path, ErrMenuPathExists},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := &model.Menu{
				ParentID: &newParent.ID, Type: model.MenuTypePage, Name: "Changed",
				RouteName: tc.routeName, Path: tc.path, Component: "changed/index",
				Sort: 0, Visible: false, KeepAlive: false, Remark: "",
			}
			if err := repo.Update(ctx, source.ID, candidate); !errors.Is(err, tc.wantErr) {
				t.Fatalf("update error=%v want=%v", err, tc.wantErr)
			}
			for _, before := range snapshots {
				after, err := repo.FindByID(ctx, before.ID)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(after, before) {
					t.Fatalf("failed update changed menu %d: before=%+v after=%+v", before.ID, before, after)
				}
			}
		})
	}
}

func TestMenuRepositoryIsolatesMobileAndRejectsCycles(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	repo, err := NewMenuRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	initial, err := repo.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mobile := model.Menu{AppCode: model.MenuAppAdminMobile, Type: model.MenuTypePage, Name: "Mobile", RouteName: "Mobile", Path: "/mobile", Component: "mobile/index"}
	if err = db.Create(&mobile).Error; err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error { _, e := repo.FindByID(ctx, mobile.ID); return e },
		func() error { return repo.UpdateStatus(ctx, mobile.ID, 0) },
		func() error { return repo.Delete(ctx, mobile.ID) },
		func() error { return repo.Update(ctx, mobile.ID, &model.Menu{Type: model.MenuTypeDirectory}) },
	} {
		if e := operation(); !errors.Is(e, ErrMenuNotFound) {
			t.Fatal(e)
		}
	}
	parent := &model.Menu{Type: model.MenuTypeDirectory, Name: "Parent", RouteName: "Parent", Path: "/parent"}
	if err = repo.Create(ctx, parent); err != nil {
		t.Fatal(err)
	}
	child := &model.Menu{ParentID: &parent.ID, Type: model.MenuTypePage, Name: "Child", RouteName: "Child", Path: "/child", Component: "child/index"}
	if err = repo.Create(ctx, child); err != nil {
		t.Fatal(err)
	}
	candidate := *parent
	candidate.ParentID = &child.ID
	if err = repo.Update(ctx, parent.ID, &candidate); !errors.Is(err, ErrMenuCycle) {
		t.Fatal(err)
	}
	candidate.ParentID = &mobile.ID
	if err = repo.Update(ctx, parent.ID, &candidate); !errors.Is(err, ErrMenuParentInvalid) {
		t.Fatal(err)
	}
	candidate.ParentID = nil
	candidate.Type = model.MenuTypeElement
	candidate.Permission = "test"
	if err = repo.Update(ctx, parent.ID, &candidate); !errors.Is(err, ErrMenuHasChildren) {
		t.Fatal(err)
	}
	list, err := repo.List(ctx)
	if err != nil || len(list) != len(initial)+2 {
		t.Fatalf("list=%+v err=%v", list, err)
	}
	for _, menu := range list {
		if menu.ID == mobile.ID {
			t.Fatal("mobile menu leaked into PC management list")
		}
	}
}
