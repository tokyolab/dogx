package logic

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListNavigationMenusFiltersSubtrees(t *testing.T) {
	node := func(id, parent int64, kind model.MenuType, enabled bool) model.Menu {
		m := model.Menu{Base: model.Base{ID: id}, AppCode: model.MenuAppAdminWeb, Type: kind, Name: "literal menu", RouteName: "Example", Path: "/example", Icon: "lucide:user", Sort: 10, Visible: true, KeepAlive: true}
		if parent != 0 {
			m.ParentID = &parent
		}
		if enabled {
			m.Status = model.RecordStatusEnabled
		}
		return m
	}
	hidden := node(3, 1, model.MenuTypePage, true)
	hidden.Visible = false
	hidden.External = true
	hidden.Path = "https://example.com"
	mobile := node(9, 0, model.MenuTypeDirectory, true)
	mobile.AppCode = model.MenuAppAdminMobile
	repo := &menuRepositoryStub{menus: []model.Menu{
		node(2, 1, model.MenuTypePage, true), // Unordered input must not lose children.
		node(1, 0, model.MenuTypeDirectory, true), hidden,
		node(4, 0, model.MenuTypeDirectory, false), node(5, 4, model.MenuTypePage, true),
		node(6, 1, model.MenuTypeElement, true), node(7, 6, model.MenuTypePage, true),
		node(8, 99, model.MenuTypePage, true), mobile, node(10, 9, model.MenuTypePage, true),
		node(11, 12, model.MenuTypeDirectory, true), node(12, 11, model.MenuTypeDirectory, true),
		node(13, 1, model.MenuTypePage, false),
	}}
	response, err := NewListNavigationMenusLogic(context.Background(), &svc.ServiceContext{MenuRepo: repo}).ListNavigationMenus(&system.ListNavigationMenusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, item := range response.Items {
		ids = append(ids, item.Id)
	}
	if !reflect.DeepEqual(ids, []int64{1, 2, 3}) {
		t.Fatalf("ids=%v", ids)
	}
	if repo.reads != 1 || repo.writes != 0 {
		t.Fatalf("reads=%d writes=%d", repo.reads, repo.writes)
	}
	item := response.Items[2]
	if item.Visible || !item.External || item.ParentId != 1 || item.Path != hidden.Path || item.Name != hidden.Name || item.Icon != hidden.Icon || item.Sort != hidden.Sort || !item.KeepAlive {
		t.Fatalf("mapping=%+v", item)
	}
}

func TestListNavigationMenusFailures(t *testing.T) {
	dbErr := errors.New("database unavailable")
	for _, tc := range []struct {
		name     string
		repo     *menuRepositoryStub
		request  *system.ListNavigationMenusRequest
		wantCode codes.Code
		wantErr  error
	}{
		{name: "nil request", repo: &menuRepositoryStub{}, wantCode: codes.InvalidArgument},
		{name: "repository failure", repo: &menuRepositoryStub{err: dbErr}, request: &system.ListNavigationMenusRequest{}, wantErr: dbErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewListNavigationMenusLogic(context.Background(), &svc.ServiceContext{MenuRepo: tc.repo}).ListNavigationMenus(tc.request)
			if err == nil || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) || (tc.wantCode != codes.OK && status.Code(err) != tc.wantCode) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	if _, err := NewListNavigationMenusLogic(context.Background(), &svc.ServiceContext{}).ListNavigationMenus(&system.ListNavigationMenusRequest{}); err == nil {
		t.Fatal("missing repository accepted")
	}
	result, err := NewListNavigationMenusLogic(context.Background(), &svc.ServiceContext{MenuRepo: &menuRepositoryStub{}}).ListNavigationMenus(&system.ListNavigationMenusRequest{})
	if err != nil || result.Items == nil || len(result.Items) != 0 {
		t.Fatalf("empty result=%v err=%v", result, err)
	}
}
