//go:build integration

package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
)

func TestDepartmentRepositoryLifecycleAndDeletionGuards(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	repo, err := NewDepartmentRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	root := &model.Department{Name: "  总部 ", Sort: 1, Status: model.RecordStatusEnabled, Remark: " 总部备注 "}
	if err := repo.Create(ctx, root); err != nil {
		t.Fatalf("create root: %v", err)
	}
	if root.ID <= 0 || root.Name != "总部" || root.CreatedAt.IsZero() {
		t.Fatalf("unexpected root: %+v", root)
	}
	child := &model.Department{ParentID: &root.ID, Name: "研发部", Sort: 2, Status: model.RecordStatusEnabled}
	if err := repo.Create(ctx, child); err != nil {
		t.Fatalf("create child: %v", err)
	}
	duplicate := &model.Department{ParentID: &root.ID, Name: "  研发部 ", Status: model.RecordStatusEnabled}
	if err := repo.Create(ctx, duplicate); !errors.Is(err, ErrDepartmentNameExists) {
		t.Fatalf("duplicate department error=%v, want %v", err, ErrDepartmentNameExists)
	}
	missingParent := int64(999999)
	if err := repo.Create(ctx, &model.Department{ParentID: &missingParent, Name: "孤儿部门"}); !errors.Is(err, ErrDepartmentParentInvalid) {
		t.Fatalf("missing parent error=%v", err)
	}
	if err := repo.Update(ctx, root.ID, &model.Department{ParentID: &child.ID, Name: root.Name, Status: root.Status}); !errors.Is(err, ErrDepartmentCycle) {
		t.Fatalf("cycle update error=%v", err)
	}
	if err := repo.Update(ctx, child.ID, &model.Department{ParentID: nil, Name: "  平台组 ", Sort: 0, Remark: "清空备注"}); err != nil {
		t.Fatalf("update child: %v", err)
	}
	updated, err := repo.FindByID(ctx, child.ID)
	if err != nil || updated.Name != "平台组" || updated.ParentID != nil || updated.Sort != 0 || updated.Remark != "清空备注" {
		t.Fatalf("updated child=%+v error=%v", updated, err)
	}
	if err := repo.UpdateStatus(ctx, child.ID, model.RecordStatusDisabled); err != nil {
		t.Fatalf("disable child: %v", err)
	}
	if got, err := repo.FindByID(ctx, child.ID); err != nil || got.Status != model.RecordStatusDisabled {
		t.Fatalf("status=%+v error=%v", got, err)
	}
	if err := repo.Delete(ctx, root.ID); err != nil {
		t.Fatalf("delete root without children: %v", err)
	}
	if _, err := repo.FindByID(ctx, root.ID); !errors.Is(err, ErrDepartmentNotFound) {
		t.Fatalf("deleted root find error=%v", err)
	}
}

func TestDepartmentRepositoryRejectsChildrenAndUserReferencesBeforeDelete(t *testing.T) {
	_, db := newPostgreSQLUserRepository(t)
	repo, err := NewDepartmentRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	parent := &model.Department{Name: "用户部门", Status: model.RecordStatusEnabled}
	if err := repo.Create(ctx, parent); err != nil {
		t.Fatal(err)
	}
	child := &model.Department{ParentID: &parent.ID, Name: "子部门", Status: model.RecordStatusEnabled}
	if err := repo.Create(ctx, child); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, parent.ID); !errors.Is(err, ErrDepartmentHasChildren) {
		t.Fatalf("parent deletion error=%v", err)
	}
	var userDepartmentID = child.ID
	user := &model.User{Username: "department-reference-user", PasswordHash: "hash", Nickname: "部门用户", DepartmentID: &userDepartmentID, Status: model.RecordStatusEnabled}
	if err := db.WithContext(ctx).Create(user).Error; err != nil {
		t.Fatalf("create referencing user: %v", err)
	}
	if err := repo.Delete(ctx, child.ID); !errors.Is(err, ErrDepartmentHasUsers) {
		t.Fatalf("referenced child deletion error=%v", err)
	}
	if err := db.WithContext(ctx).Model(user).Update("department_id", nil).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, child.ID); err != nil {
		t.Fatalf("delete unreferenced child: %v", err)
	}
	if err := repo.Delete(ctx, parent.ID); err != nil {
		t.Fatalf("delete unreferenced parent: %v", err)
	}
	if err := repo.Delete(ctx, parent.ID); !errors.Is(err, ErrDepartmentNotFound) {
		t.Fatalf("repeat deletion error=%v", err)
	}
}
