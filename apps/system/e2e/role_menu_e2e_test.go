//go:build integration && e2e

package e2e

import (
	"net/http"
	"slices"
	"testing"
)

func testRoleMenuAuthorization(t *testing.T, client *http.Client, baseURL, adminToken, userToken string, roleID, superRoleID int64) {
	t.Helper()
	call := func(token, path string, body any) responseEnvelope {
		t.Helper()
		code, res := postJSON(t, client, baseURL+path, token, body)
		assertEnvelope(t, code, res, http.StatusOK, 0, "")
		return res
	}
	rootBody := map[string]any{"parentId": 0, "type": 1, "name": "Grant root", "routeName": "E2EGrantRoot", "path": "/e2e-grants", "sort": 0, "visible": true, "keepAlive": false, "external": false, "status": 1}
	var root, page, view struct {
		ID int64 `json:"id"`
	}
	decodeData(t, call(adminToken, "/menu/create", rootBody), &root)
	decodeData(t, call(adminToken, "/menu/create", map[string]any{"parentId": root.ID, "type": 2, "name": "Grant page", "routeName": "E2EGrantPage", "path": "/e2e-grants/page", "component": "system/user/index", "sort": 0, "visible": true, "keepAlive": false, "external": false, "status": 1}), &page)
	decodeData(t, call(adminToken, "/menu/create", map[string]any{"parentId": page.ID, "type": 3, "name": "View", "permission": "e2e.grants.view", "sort": 0, "visible": false, "keepAlive": false, "external": false, "status": 1}), &view)
	check := func(want []int64, codes []string) {
		t.Helper()
		var nav struct {
			Items []struct {
				ID int64 `json:"id"`
			} `json:"items"`
			Permissions []string `json:"permissions"`
		}
		decodeData(t, call(userToken, "/auth/menus", map[string]any{"isSuperAdmin": true, "roleIds": []int64{superRoleID}}), &nav)
		ids := make([]int64, 0)
		for _, item := range nav.Items {
			ids = append(ids, item.ID)
		}
		if !slices.Equal(ids, want) || !slices.Equal(nav.Permissions, codes) {
			t.Fatalf("navigation=%+v", nav)
		}
	}
	check(nil, nil)
	call(adminToken, "/role/menu/update", map[string]any{"roleId": roleID, "menuIds": []int64{view.ID}})
	check([]int64{root.ID, page.ID}, []string{"e2e.grants.view"})
	var grants struct {
		MenuIDs []int64 `json:"menuIds"`
	}
	decodeData(t, call(adminToken, "/role/menu/get", map[string]any{"roleId": roleID}), &grants)
	if !slices.Equal(grants.MenuIDs, []int64{root.ID, page.ID, view.ID}) {
		t.Fatalf("grants=%v", grants)
	}
	// The same session immediately sees grants, but menu management is still forbidden.
	code, res := postJSON(t, client, baseURL+"/menu/list", userToken, nil)
	assertEnvelope(t, code, res, http.StatusForbidden, http.StatusForbidden, "common.permission_denied")
	call(adminToken, "/menu/status/update", map[string]any{"id": root.ID, "status": 0})
	check(nil, nil)
	call(adminToken, "/menu/status/update", map[string]any{"id": root.ID, "status": 1})
	call(adminToken, "/role/menu/update", map[string]any{"roleId": roleID, "menuIds": []int64{}})
	check(nil, nil)
	// API grants survive clearing menu grants.
	call(userToken, "/role/list", map[string]any{"page": 1, "pageSize": 20})
	call(adminToken, "/menu/delete", map[string]any{"id": view.ID})
	call(adminToken, "/menu/delete", map[string]any{"id": page.ID})
	call(adminToken, "/menu/delete", map[string]any{"id": root.ID})
}
