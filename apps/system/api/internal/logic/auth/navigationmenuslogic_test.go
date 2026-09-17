package auth

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNavigationMenus(t *testing.T) {
	rpc := &systemRPCStub{navigationResponse: &systemclient.ListNavigationMenusResponse{Items: []*systemclient.NavigationMenu{{Id: 2, ParentId: 1, Type: 2, Name: "原样显示", RouteName: "Example", Path: "/example", Component: "system/user/index", Icon: "lucide:user", Sort: 3, Visible: false, KeepAlive: true, External: false}}}}
	response, err := NewNavigationMenusLogic(authenticatedTestContext(), &svc.ServiceContext{SystemRpc: rpc}).NavigationMenus()
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 || rpc.navigationCalls != 1 {
		t.Fatalf("response=%+v calls=%d", response, rpc.navigationCalls)
	}
	item := response.Items[0]
	if item.Id != 2 || item.ParentId != 1 || item.Type != 2 || item.Name != "原样显示" || item.RouteName != "Example" || item.Path != "/example" || item.Component != "system/user/index" || item.Icon != "lucide:user" || item.Sort != 3 || item.Visible || !item.KeepAlive || item.External {
		t.Fatalf("mapping=%+v", item)
	}
	rpc.navigationResponse.Items = nil
	response, err = NewNavigationMenusLogic(authenticatedTestContext(), &svc.ServiceContext{SystemRpc: rpc}).NavigationMenus()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(response)
	if err != nil || !strings.Contains(string(data), `"items":[]`) {
		t.Fatalf("empty response=%s err=%v", data, err)
	}
}

func TestNavigationMenusFailures(t *testing.T) {
	rpc := &systemRPCStub{}
	_, err := NewNavigationMenusLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc}).NavigationMenus()
	if status.Code(err) != codes.Unauthenticated || rpc.navigationCalls != 0 {
		t.Fatalf("unauthenticated err=%v calls=%d", err, rpc.navigationCalls)
	}
	rpc.err = errors.New("rpc unavailable")
	_, err = NewNavigationMenusLogic(authenticatedTestContext(), &svc.ServiceContext{SystemRpc: rpc}).NavigationMenus()
	if !errors.Is(err, rpc.err) {
		t.Fatalf("err=%v", err)
	}
	rpc.err = nil
	for _, result := range []*systemclient.ListNavigationMenusResponse{nil, {Items: []*systemclient.NavigationMenu{nil}}} {
		rpc.navigationResponse = result
		if _, err = NewNavigationMenusLogic(authenticatedTestContext(), &svc.ServiceContext{SystemRpc: rpc}).NavigationMenus(); err == nil {
			t.Fatal("invalid RPC response accepted")
		}
	}
}
