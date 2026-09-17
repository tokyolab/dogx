package menu

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"github.com/tokyolab/dogx/pkg/requestvalidator"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMenuHTTPEmbeddedFieldsAreParsedAndValidated(t *testing.T) {
	request := httptest.NewRequest("POST", "/menu/create", strings.NewReader(`{
		"parentId": 0, "type": 1, "name": "目录", "routeName": "Root", "path": "/root",
		"sort": 0, "visible": false, "keepAlive": false, "external": false, "status": 0
	}`))
	request.Header.Set("Content-Type", "application/json")
	var input types.CreateMenuReq
	if err := httpx.ParseJsonBody(request, &input); err != nil {
		t.Fatal(err)
	}
	if input.Name != "目录" || input.Type != 1 || input.RouteName != "Root" || input.Status != 0 || input.Visible {
		t.Fatalf("embedded fields were not parsed: %+v", input)
	}
	validator := requestvalidator.New()
	if err := validator.Validate(request, &input); err != nil {
		t.Fatal(err)
	}
	input.Type = 4
	if err := validator.Validate(request, &input); err == nil {
		t.Fatal("embedded type validation was skipped")
	}
	input.Type = 1
	input.Name = strings.Repeat("中", 65)
	if err := validator.Validate(request, &input); err == nil {
		t.Fatal("embedded name length validation was skipped")
	}
}

func TestMenuHTTPLogicForwardsAllFieldsAndZeroValues(t *testing.T) {
	fields := types.MenuFields{ParentId: 1, Type: 2, Name: "页面", RouteName: "UserPage", Path: "/user", Component: "system/user/index", Permission: "", Icon: "lucide:user", Sort: 0, Visible: false, KeepAlive: false, External: false, Remark: ""}
	rpc := &systemRPCStub{item: &systemclient.MenuInfo{Id: 9, Menu: toMenuFields(fields), Status: 0}}
	rpc.items = []*systemclient.MenuInfo{rpc.item}
	s := &svc.ServiceContext{SystemRpc: rpc}
	ctx := context.Background()
	result, err := NewCreateMenuLogic(ctx, s).CreateMenu(&types.CreateMenuReq{MenuFields: fields, Status: 0})
	if err != nil || result.Id != 9 || !reflect.DeepEqual(rpc.create.Menu, toMenuFields(fields)) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err = NewUpdateMenuLogic(ctx, s).UpdateMenu(&types.UpdateMenuReq{Id: 9, MenuFields: fields}); err != nil || rpc.update.Id != 9 || !reflect.DeepEqual(rpc.update.Menu, toMenuFields(fields)) {
		t.Fatal(err)
	}
	if _, err = NewUpdateMenuStatusLogic(ctx, s).UpdateMenuStatus(&types.UpdateMenuStatusReq{Id: 9, Status: 0}); err != nil || rpc.status.Status != 0 {
		t.Fatal(err)
	}
	item, err := NewGetMenuLogic(ctx, s).GetMenu(&types.IDReq{Id: 9})
	if err != nil || !reflect.DeepEqual(item.MenuFields, fields) {
		t.Fatalf("item=%+v err=%v", item, err)
	}
	list, err := NewListMenusLogic(ctx, s).ListMenus()
	if err != nil || len(list.Items) != 1 || list.Items[0].Id != 9 {
		t.Fatal(err)
	}
	if _, err = NewDeleteMenuLogic(ctx, s).DeleteMenu(&types.IDReq{Id: 9}); err != nil || rpc.id != 9 {
		t.Fatal(err)
	}
}
func TestMenuHTTPLogicFailures(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("RPC unavailable")
	s := &svc.ServiceContext{SystemRpc: &systemRPCStub{err: sentinel}}
	calls := []func() error{
		func() error { _, e := NewCreateMenuLogic(ctx, s).CreateMenu(&types.CreateMenuReq{}); return e },
		func() error { _, e := NewUpdateMenuLogic(ctx, s).UpdateMenu(&types.UpdateMenuReq{Id: 1}); return e },
		func() error {
			_, e := NewUpdateMenuStatusLogic(ctx, s).UpdateMenuStatus(&types.UpdateMenuStatusReq{Id: 1})
			return e
		},
		func() error { _, e := NewGetMenuLogic(ctx, s).GetMenu(&types.IDReq{Id: 1}); return e },
		func() error { _, e := NewListMenusLogic(ctx, s).ListMenus(); return e },
		func() error { _, e := NewDeleteMenuLogic(ctx, s).DeleteMenu(&types.IDReq{Id: 1}); return e },
	}
	for _, call := range calls {
		if !errors.Is(call(), sentinel) {
			t.Fatal("RPC error was lost")
		}
	}
	for _, item := range []*systemclient.MenuInfo{nil, {}, {Id: 1}} {
		if _, err := toMenuItem(item); status.Code(err) != codes.Internal {
			t.Fatalf("bad item=%+v err=%v", item, err)
		}
	}
	s.SystemRpc = &systemRPCStub{}
	result, err := NewListMenusLogic(ctx, s).ListMenus()
	if err != nil || result.Items == nil || len(result.Items) != 0 {
		t.Fatal("empty list should encode as []")
	}
}
