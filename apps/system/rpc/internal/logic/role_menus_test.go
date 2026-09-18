package logic

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRoleMenuReadAndReplace(t *testing.T) {
	ctx := context.Background()
	grants := &roleMenuRepositoryStub{ids: []int64{1, 2}}
	roles := &roleRepositoryStub{role: &model.Role{Code: "reader"}}
	menus := &menuRepositoryStub{menus: []model.Menu{{Base: model.Base{ID: 1}, Name: "Root"}, {Base: model.Base{ID: 2}, Name: "Page"}}}
	s := &svc.ServiceContext{RoleRepo: roles, MenuRepo: menus, RoleMenuRepo: grants}
	got, err := NewGetRoleMenusLogic(ctx, s).GetRoleMenus(&system.GetRoleMenusRequest{RoleId: 9})
	if err != nil || len(got.Items) != 2 || !slices.Equal(got.MenuIds, grants.ids) || roles.findID != 9 || !slices.Equal(grants.roles, []int64{9}) {
		t.Fatalf("got=%v err=%v", got, err)
	}
	roles.role.Code = model.SuperAdminRoleCode
	grants.err = errors.New("must bypass grants")
	got, err = NewGetRoleMenusLogic(ctx, s).GetRoleMenus(&system.GetRoleMenusRequest{RoleId: 9})
	if err != nil || !slices.Equal(got.MenuIds, []int64{1, 2}) || grants.reads != 1 {
		t.Fatalf("super=%v err=%v", got, err)
	}
	grants.err = nil
	_, err = NewReplaceRoleMenusLogic(ctx, s).ReplaceRoleMenus(&system.ReplaceRoleMenusRequest{RoleId: 9, MenuIds: []int64{2}})
	if err != nil || grants.roleID != 9 || !slices.Equal(grants.saved, []int64{2}) {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		err  error
		code string
	}{
		{repository.ErrRoleNotFound, subcode.RoleNotFound},
		{repository.ErrRoleMenuRoleDisabled, subcode.RoleUnavailable},
		{repository.ErrRoleMenuUnavailable, subcode.RoleMenuUnavailable},
		{repository.ErrSuperAdminMenusProtected, subcode.RoleSuperAdminMenuProtected},
	} {
		grants.err = tc.err
		_, err = NewReplaceRoleMenusLogic(ctx, s).ReplaceRoleMenus(&system.ReplaceRoleMenusRequest{RoleId: 9})
		if !hasBusinessSubcode(err, tc.code) {
			t.Fatalf("err=%v want=%s", err, tc.code)
		}
	}
	dbErr := errors.New("database unavailable")
	roles.findErr = dbErr
	if _, err = NewGetRoleMenusLogic(ctx, s).GetRoleMenus(&system.GetRoleMenusRequest{RoleId: 9}); !errors.Is(err, dbErr) {
		t.Fatal(err)
	}
	roles.findErr = repository.ErrRoleNotFound
	if _, err = NewGetRoleMenusLogic(ctx, s).GetRoleMenus(&system.GetRoleMenusRequest{RoleId: 9}); !hasBusinessSubcode(err, subcode.RoleNotFound) {
		t.Fatal(err)
	}
	roles.findErr = nil
	roles.role.Code = "reader"
	menus.err = dbErr
	if _, err = NewGetRoleMenusLogic(ctx, s).GetRoleMenus(&system.GetRoleMenusRequest{RoleId: 9}); !errors.Is(err, dbErr) {
		t.Fatal(err)
	}
	menus.err = nil
	grants.err = dbErr
	if _, err = NewGetRoleMenusLogic(ctx, s).GetRoleMenus(&system.GetRoleMenusRequest{RoleId: 9}); !errors.Is(err, dbErr) {
		t.Fatal(err)
	}
	if _, err = NewReplaceRoleMenusLogic(ctx, s).ReplaceRoleMenus(&system.ReplaceRoleMenusRequest{RoleId: 9}); !errors.Is(err, dbErr) {
		t.Fatal(err)
	}
}
func TestRoleMenuInvalidRequestsDoNotWrite(t *testing.T) {
	ctx := context.Background()
	grants := &roleMenuRepositoryStub{}
	s := &svc.ServiceContext{RoleMenuRepo: grants}
	for _, req := range []*system.ReplaceRoleMenusRequest{nil, {}, {RoleId: 1, MenuIds: []int64{0}}, {RoleId: 1, MenuIds: make([]int64, 10001)}} {
		if _, err := NewReplaceRoleMenusLogic(ctx, s).ReplaceRoleMenus(req); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	if grants.writes != 0 {
		t.Fatal("invalid request wrote grants")
	}
	for _, req := range []*system.GetRoleMenusRequest{nil, {}} {
		if _, err := NewGetRoleMenusLogic(ctx, s).GetRoleMenus(req); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	if _, err := NewGetRoleMenusLogic(ctx, &svc.ServiceContext{}).GetRoleMenus(&system.GetRoleMenusRequest{RoleId: 1}); err == nil {
		t.Fatal("missing dependencies accepted")
	}
	if _, err := NewReplaceRoleMenusLogic(ctx, &svc.ServiceContext{}).ReplaceRoleMenus(&system.ReplaceRoleMenusRequest{RoleId: 1}); err == nil {
		t.Fatal("missing dependencies accepted")
	}
}
func TestNavigationUsesExplicitRoleUnionAndElementCodes(t *testing.T) {
	root, page := int64(1), int64(2)
	menus := &menuRepositoryStub{menus: []model.Menu{
		{Base: model.Base{ID: 1}, AppCode: model.MenuAppAdminWeb, Type: 1, Status: 1},
		{Base: model.Base{ID: 2}, AppCode: model.MenuAppAdminWeb, Type: 2, Status: 1, ParentID: &root},
		{Base: model.Base{ID: 3}, AppCode: model.MenuAppAdminWeb, Type: 3, Status: 1, ParentID: &page, Permission: "user.view"},
		{Base: model.Base{ID: 4}, AppCode: model.MenuAppAdminWeb, Type: 3, Status: 1, ParentID: &page, Permission: "user.delete"},
	}}
	grants := &roleMenuRepositoryStub{ids: []int64{1, 2, 3}}
	s := &svc.ServiceContext{MenuRepo: menus, RoleMenuRepo: grants}
	ctx := context.Background()
	check := func(req *system.ListNavigationMenusRequest, want []int64, permissions []string) {
		t.Helper()
		got, err := NewListNavigationMenusLogic(ctx, s).ListNavigationMenus(req)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]int64, 0)
		for _, item := range got.Items {
			ids = append(ids, item.Id)
		}
		if !slices.Equal(ids, want) || !slices.Equal(got.Permissions, permissions) {
			t.Fatalf("got=%v", got)
		}
	}
	check(&system.ListNavigationMenusRequest{RoleIds: []int64{8, 9}}, []int64{1, 2}, []string{"user.view"})
	if !slices.Equal(grants.roles, []int64{8, 9}) || grants.reads != 1 {
		t.Fatal("not a single role union query")
	}
	check(&system.ListNavigationMenusRequest{}, nil, nil)
	check(&system.ListNavigationMenusRequest{IsSuperAdmin: true}, []int64{1, 2}, []string{"user.view", "user.delete"})
	menus.menus[0].Status = 0
	check(&system.ListNavigationMenusRequest{RoleIds: []int64{8}}, nil, nil)
	menus.menus[0].Status = 1
	grants.ids = []int64{2, 3}
	check(&system.ListNavigationMenusRequest{RoleIds: []int64{8}}, nil, nil)
	grants.err = errors.New("grants unavailable")
	if _, err := NewListNavigationMenusLogic(ctx, s).ListNavigationMenus(&system.ListNavigationMenusRequest{RoleIds: []int64{8}}); !errors.Is(err, grants.err) {
		t.Fatal(err)
	}
	if _, err := NewListNavigationMenusLogic(ctx, s).ListNavigationMenus(&system.ListNavigationMenusRequest{RoleIds: []int64{0}}); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	s.RoleMenuRepo = nil
	if _, err := NewListNavigationMenusLogic(ctx, s).ListNavigationMenus(&system.ListNavigationMenusRequest{RoleIds: []int64{8}}); err == nil {
		t.Fatal("missing grants accepted")
	}
}
