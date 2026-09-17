package logic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validMenuFields() *system.MenuFields {
	return &system.MenuFields{Type: 2, Name: " 页面 ", RouteName: "TestPage", Path: "/test", Component: "system/test/index", Visible: true}
}
func TestMenuInputValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*system.MenuFields)
	}{
		{"name required", func(m *system.MenuFields) { m.Name = "  " }},
		{"name too long", func(m *system.MenuFields) { m.Name = strings.Repeat("中", 65) }},
		{"sort", func(m *system.MenuFields) { m.Sort = -1 }},
		{"parent", func(m *system.MenuFields) { m.ParentId = -1 }},
		{"enum truncation", func(m *system.MenuFields) { m.Type = 65538 }},
		{"enum negative", func(m *system.MenuFields) { m.Type = -65534 }},
		{"enum zero", func(m *system.MenuFields) { m.Type = 0 }},
		{"route name", func(m *system.MenuFields) { m.RouteName = "12-abc" }},
		{"blank path", func(m *system.MenuFields) { m.Path = "" }},
		{"protocol relative path", func(m *system.MenuFields) { m.Path = "//host" }},
		{"path query", func(m *system.MenuFields) { m.Path = "/test?q=1" }},
		{"blank component", func(m *system.MenuFields) { m.Component = "" }},
		{"component traversal", func(m *system.MenuFields) { m.Component = "../secret" }},
		{"permission", func(m *system.MenuFields) { m.Type = 3; m.Permission = "" }},
		{"permission whitespace", func(m *system.MenuFields) { m.Type = 3; m.Permission = "user create" }},
		{"external script", func(m *system.MenuFields) { m.External = true; m.Path = "javascript:alert(1)" }},
		{"external userinfo", func(m *system.MenuFields) { m.External = true; m.Path = "https://user:pass@example.com" }},
		{"external no host", func(m *system.MenuFields) { m.External = true; m.Path = "https:///path" }},
		{"nul", func(m *system.MenuFields) { m.Remark = "a\x00b" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := validMenuFields()
			tc.change(m)
			if _, err := normalizeMenuInput(m); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if _, err := normalizeMenuInput(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	m := validMenuFields()
	m.Name = strings.Repeat("中", 64)
	if _, err := normalizeMenuInput(m); err != nil {
		t.Fatal(err)
	}
}
func TestMenuTypeChangeClearsInapplicableFields(t *testing.T) {
	m := validMenuFields()
	m.Type = 3
	m.ParentId = 1
	m.Permission = "system.user.create"
	m.External = true
	m.KeepAlive = true
	m.Icon = "lucide:user"
	element, err := normalizeMenuInput(m)
	if err != nil || element.Path != "" || element.RouteName != "" || element.Component != "" || element.Icon != "" || element.Visible || element.KeepAlive || element.External || element.Permission == "" {
		t.Fatalf("element=%+v err=%v", element, err)
	}
	m = validMenuFields()
	m.Type = 1
	m.Permission = "stale"
	m.External = true
	m.KeepAlive = true
	directory, err := normalizeMenuInput(m)
	if err != nil || directory.Component != "" || directory.Permission != "" || directory.External || directory.KeepAlive {
		t.Fatalf("directory=%+v err=%v", directory, err)
	}
	m = validMenuFields()
	m.External = true
	m.Path = "https://example.com/a?q=1"
	m.KeepAlive = true
	external, err := normalizeMenuInput(m)
	if err != nil || external.Component != "" || external.KeepAlive || !external.External {
		t.Fatalf("external=%+v err=%v", external, err)
	}
}
func TestMenuLogicCRUD(t *testing.T) {
	ctx := context.Background()
	repo := &menuRepositoryStub{}
	services := &svc.ServiceContext{MenuRepo: repo}
	created, err := NewCreateMenuLogic(ctx, services).CreateMenu(&system.CreateMenuRequest{Menu: validMenuFields(), Status: 0})
	if err != nil || created.Id != 42 || repo.menu.Name != "页面" || repo.menu.Status != 0 {
		t.Fatalf("created=%+v repo=%+v err=%v", created, repo, err)
	}
	if _, err = NewUpdateMenuLogic(ctx, services).UpdateMenu(&system.UpdateMenuRequest{Id: 42, Menu: validMenuFields()}); err != nil || repo.id != 42 {
		t.Fatal(err)
	}
	if _, err = NewUpdateMenuStatusLogic(ctx, services).UpdateMenuStatus(&system.UpdateMenuStatusRequest{Id: 42, Status: 1}); err != nil || repo.status != 1 {
		t.Fatal(err)
	}
	repo.menu.ID = 42
	found, err := NewGetMenuLogic(ctx, services).GetMenu(&system.GetMenuRequest{Id: 42})
	if err != nil || found.Menu.Menu.Name != "页面" {
		t.Fatal(err)
	}
	repo.menus = []model.Menu{*repo.menu}
	listed, err := NewListMenusLogic(ctx, services).ListMenus(&system.ListMenusRequest{})
	if err != nil || len(listed.Items) != 1 {
		t.Fatal(err)
	}
	if _, err = NewDeleteMenuLogic(ctx, services).DeleteMenu(&system.DeleteMenuRequest{Id: 42}); err != nil || repo.writes != 4 {
		t.Fatal(err)
	}
}
func TestMenuStatusRejectsNarrowingOverflow(t *testing.T) {
	ctx := context.Background()
	for _, value := range []int32{-1, 2, 65536, 65537, -65536, 2147483647} {
		repo := &menuRepositoryStub{}
		s := &svc.ServiceContext{MenuRepo: repo}
		_, createErr := NewCreateMenuLogic(ctx, s).CreateMenu(&system.CreateMenuRequest{Menu: validMenuFields(), Status: value})
		_, statusErr := NewUpdateMenuStatusLogic(ctx, s).UpdateMenuStatus(&system.UpdateMenuStatusRequest{Id: 1, Status: value})
		if status.Code(createErr) != codes.InvalidArgument || status.Code(statusErr) != codes.InvalidArgument || repo.writes != 0 {
			t.Fatalf("status=%d create=%v update=%v writes=%d", value, createErr, statusErr, repo.writes)
		}
	}
}
func TestMenuErrorsPreserveStableSubcodes(t *testing.T) {
	tests := []struct {
		err  error
		code string
	}{
		{repository.ErrMenuNotFound, subcode.MenuNotFound},
		{repository.ErrMenuParentInvalid, subcode.MenuParentInvalid},
		{repository.ErrMenuCycle, subcode.MenuCycle},
		{repository.ErrMenuHasChildren, subcode.MenuHasChildren},
		{repository.ErrMenuRouteNameExists, subcode.MenuRouteNameExists},
		{repository.ErrMenuPathExists, subcode.MenuPathExists},
	}
	for _, tc := range tests {
		err := menuBusinessError(fmt.Errorf("wrapped: %w", tc.err))
		biz, ok := bizerror.From(err)
		if !ok || biz.Subcode() != tc.code {
			t.Fatalf("error=%v code=%s", err, tc.code)
		}
	}
	sentinel := errors.New("database unavailable")
	if !errors.Is(menuBusinessError(sentinel), sentinel) {
		t.Fatal("technical error lost")
	}
}
func TestMenuLogicRejectsNilAndPropagatesFailures(t *testing.T) {
	ctx := context.Background()
	s := &svc.ServiceContext{}
	checks := []func() error{
		func() error { _, e := NewCreateMenuLogic(ctx, s).CreateMenu(nil); return e },
		func() error { _, e := NewUpdateMenuLogic(ctx, s).UpdateMenu(nil); return e },
		func() error { _, e := NewUpdateMenuStatusLogic(ctx, s).UpdateMenuStatus(nil); return e },
		func() error { _, e := NewDeleteMenuLogic(ctx, s).DeleteMenu(nil); return e },
		func() error { _, e := NewGetMenuLogic(ctx, s).GetMenu(nil); return e },
		func() error { _, e := NewListMenusLogic(ctx, s).ListMenus(nil); return e },
	}
	for _, check := range checks {
		if status.Code(check()) != codes.InvalidArgument {
			t.Fatal("nil request accepted")
		}
	}
	sentinel := errors.New("db failure")
	s.MenuRepo = &menuRepositoryStub{err: sentinel}
	failures := []func() error{
		func() error {
			_, e := NewCreateMenuLogic(ctx, s).CreateMenu(&system.CreateMenuRequest{Menu: validMenuFields()})
			return e
		},
		func() error {
			_, e := NewUpdateMenuLogic(ctx, s).UpdateMenu(&system.UpdateMenuRequest{Id: 1, Menu: validMenuFields()})
			return e
		},
		func() error {
			_, e := NewUpdateMenuStatusLogic(ctx, s).UpdateMenuStatus(&system.UpdateMenuStatusRequest{Id: 1})
			return e
		},
		func() error {
			_, e := NewDeleteMenuLogic(ctx, s).DeleteMenu(&system.DeleteMenuRequest{Id: 1})
			return e
		},
		func() error { _, e := NewGetMenuLogic(ctx, s).GetMenu(&system.GetMenuRequest{Id: 1}); return e },
		func() error { _, e := NewListMenusLogic(ctx, s).ListMenus(&system.ListMenusRequest{}); return e },
	}
	for _, check := range failures {
		if !errors.Is(check(), sentinel) {
			t.Fatal("repository failure lost")
		}
	}
}
