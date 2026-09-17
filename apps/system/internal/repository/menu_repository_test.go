package repository

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
)

func TestMenuTreeValidation(t *testing.T) {
	one, two := int64(1), int64(2)
	nodes := []model.Menu{{Base: model.Base{ID: 1}, Type: model.MenuTypeDirectory}, {Base: model.Base{ID: 2}, ParentID: &one, Type: model.MenuTypePage}, {Base: model.Base{ID: 3}, ParentID: &two, Type: model.MenuTypeElement}}
	tests := []struct {
		name       string
		id, parent int64
		kind       model.MenuType
		want       error
	}{
		{"new root", 0, 0, model.MenuTypeDirectory, nil},
		{"page under directory", 0, 1, model.MenuTypePage, nil},
		{"element under page", 0, 2, model.MenuTypeElement, nil},
		{"missing parent", 0, 10, model.MenuTypePage, ErrMenuParentInvalid},
		{"element parent", 0, 3, model.MenuTypeElement, ErrMenuParentInvalid},
		{"root element", 0, 0, model.MenuTypeElement, ErrMenuParentInvalid},
		{"self", 2, 2, model.MenuTypePage, ErrMenuCycle},
		{"descendant", 1, 2, model.MenuTypeDirectory, ErrMenuCycle},
		{"parent converted", 2, 1, model.MenuTypeElement, ErrMenuHasChildren},
		{"leaf converted", 3, 2, model.MenuTypePage, nil},
		{"missing update", 10, 0, model.MenuTypePage, ErrMenuNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &model.Menu{Type: tc.kind}
			if tc.parent > 0 {
				m.ParentID = &tc.parent
			}
			if err := validateMenuTree(nodes, tc.id, m); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
		})
	}
	nodes[0].ParentID = &two
	if err := validateMenuTree(nodes, 0, &model.Menu{Type: model.MenuTypePage, ParentID: &one}); !errors.Is(err, ErrMenuCycle) {
		t.Fatalf("preexisting cycle: %v", err)
	}
}
func TestMenuUniqueConstraintMapping(t *testing.T) {
	for name, want := range map[string]error{"uk_sys_menu_app_route_name_active": ErrMenuRouteNameExists, "uk_sys_menu_app_path_active": ErrMenuPathExists} {
		if got := mapMenuWriteError(&pgconn.PgError{Code: "23505", ConstraintName: name}); !errors.Is(got, want) {
			t.Fatal(got)
		}
	}
	sentinel := errors.New("db failure")
	if !errors.Is(mapMenuWriteError(sentinel), sentinel) {
		t.Fatal("lost db error")
	}
	if mapMenuWriteError(nil) != nil {
		t.Fatal("nil error")
	}
	if _, err := NewMenuRepository(nil); err == nil {
		t.Fatal("nil db")
	}
}
