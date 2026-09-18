package role

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRoleMenuHTTPMapping(t *testing.T) {
	ctx := context.Background()
	rpc := &roleQuerySystemRPCStub{menuResponse: &systemclient.GetRoleMenusResponse{Items: []*systemclient.MenuInfo{{Id: 1, Menu: &systemclient.MenuFields{Name: "Root"}}}, MenuIds: []int64{1}}}
	svcCtx := &svc.ServiceContext{SystemRpc: rpc}
	got, err := NewGetRoleMenusLogic(ctx, svcCtx).GetRoleMenus(&types.GetRoleMenusReq{RoleId: 9})
	if err != nil || rpc.menuRequest.RoleId != 9 || len(got.Items) != 1 || got.Items[0].Name != "Root" || !slices.Equal(got.MenuIds, []int64{1}) {
		t.Fatalf("got=%v err=%v", got, err)
	}
	rpc.menuResponse = &systemclient.GetRoleMenusResponse{}
	got, err = NewGetRoleMenusLogic(ctx, svcCtx).GetRoleMenus(&types.GetRoleMenusReq{RoleId: 9})
	if err != nil || got.Items == nil || got.MenuIds == nil {
		t.Fatal("null empty response", err)
	}
	_, err = NewUpdateRoleMenusLogic(ctx, svcCtx).UpdateRoleMenus(&types.UpdateRoleMenusReq{RoleId: 9, MenuIds: []int64{1, 2}})
	if err != nil || rpc.replaceMenuRequest.RoleId != 9 || !slices.Equal(rpc.replaceMenuRequest.MenuIds, []int64{1, 2}) {
		t.Fatal(err)
	}
	for _, response := range []*systemclient.GetRoleMenusResponse{nil, {Items: []*systemclient.MenuInfo{nil}}} {
		rpc.menuResponse = response
		if _, err = NewGetRoleMenusLogic(ctx, svcCtx).GetRoleMenus(&types.GetRoleMenusReq{RoleId: 9}); status.Code(err) != codes.Internal {
			t.Fatal(err)
		}
	}
	if _, err = NewGetRoleMenusLogic(ctx, svcCtx).GetRoleMenus(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err = NewUpdateRoleMenusLogic(ctx, svcCtx).UpdateRoleMenus(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	rpc.err = errors.New("RPC unavailable")
	if _, err = NewGetRoleMenusLogic(ctx, svcCtx).GetRoleMenus(&types.GetRoleMenusReq{RoleId: 9}); !errors.Is(err, rpc.err) {
		t.Fatal(err)
	}
	if _, err = NewUpdateRoleMenusLogic(ctx, svcCtx).UpdateRoleMenus(&types.UpdateRoleMenusReq{RoleId: 9}); !errors.Is(err, rpc.err) {
		t.Fatal(err)
	}
}
