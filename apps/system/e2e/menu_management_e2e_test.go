//go:build integration && e2e

package e2e

import (
	"net/http"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/pkg/bizerror"
)

func testMenuManagement(t *testing.T, client *http.Client, baseURL, token string) {
	t.Helper()
	call := func(path string, body any) responseEnvelope {
		t.Helper()
		code, res := postJSON(t, client, baseURL+path, token, body)
		assertEnvelope(t, code, res, http.StatusOK, 0, "")
		return res
	}
	fields := func(name, route, path string, parent int64, kind int) map[string]any {
		return map[string]any{"parentId": parent, "type": kind, "name": name, "routeName": route, "path": path, "component": "system/user/index", "permission": "", "icon": "", "sort": 0, "visible": true, "keepAlive": false, "external": false, "remark": "", "status": 1}
	}
	rootBody := fields("E2E root", "E2ERoot", "/e2e-menu", 0, 1)
	var root, leaf struct {
		ID int64 `json:"id"`
	}
	decodeData(t, call("/menu/create", rootBody), &root)
	pageBody := fields("E2E page", "E2EPage", "/e2e-menu/page", root.ID, 2)
	decodeData(t, call("/menu/create", pageBody), &leaf)
	checkNavigation := func(want bool) {
		t.Helper()
		var navigation struct {
			Items []struct {
				ID      int64 `json:"id"`
				Visible bool  `json:"visible"`
			} `json:"items"`
		}
		decodeData(t, call("/auth/menus", nil), &navigation)
		found := false
		for _, item := range navigation.Items {
			if item.ID == leaf.ID {
				found = true
				if item.Visible {
					t.Fatal("hidden menu became visible")
				}
			}
		}
		if found != want {
			t.Fatalf("navigation contains child=%v want=%v", found, want)
		}
	}
	pageBody["id"] = leaf.ID
	pageBody["visible"] = false
	call("/menu/update", pageBody)
	checkNavigation(true)
	call("/menu/status/update", map[string]any{"id": root.ID, "status": 0})
	checkNavigation(false)
	call("/menu/status/update", map[string]any{"id": root.ID, "status": 1})
	checkNavigation(true)
	code, res := postJSON(t, client, baseURL+"/menu/delete", token, map[string]any{"id": root.ID})
	assertEnvelope(t, code, res, http.StatusOK, bizerror.DefaultCode, subcode.MenuHasChildren)
	rootBody["id"] = root.ID
	rootBody["parentId"] = leaf.ID
	code, res = postJSON(t, client, baseURL+"/menu/update", token, rootBody)
	assertEnvelope(t, code, res, http.StatusOK, bizerror.DefaultCode, subcode.MenuCycle)
	pageBody["id"] = leaf.ID
	pageBody["type"] = 3
	pageBody["permission"] = "e2e.menu.create"
	call("/menu/update", pageBody)
	checkNavigation(false)
	var detail struct {
		ID         int64  `json:"id"`
		Type       int32  `json:"type"`
		Path       string `json:"path"`
		Component  string `json:"component"`
		Visible    bool   `json:"visible"`
		Permission string `json:"permission"`
		Status     int32  `json:"status"`
	}
	decodeData(t, call("/menu/get", map[string]any{"id": leaf.ID}), &detail)
	if detail.Type != 3 || detail.Path != "" || detail.Component != "" || detail.Visible || detail.Permission != "e2e.menu.create" {
		t.Fatalf("type conversion=%+v", detail)
	}
	call("/menu/status/update", map[string]any{"id": leaf.ID, "status": 0})
	decodeData(t, call("/menu/get", map[string]any{"id": leaf.ID}), &detail)
	if detail.Status != 0 {
		t.Fatal("menu was not disabled")
	}
	var listed struct {
		Items []struct {
			ID int64 `json:"id"`
		} `json:"items"`
	}
	decodeData(t, call("/menu/list", nil), &listed)
	found := false
	for _, item := range listed.Items {
		if item.ID == leaf.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("disabled menu missing from management list")
	}
	call("/menu/delete", map[string]any{"id": leaf.ID})
	call("/menu/delete", map[string]any{"id": root.ID})
	code, res = postJSON(t, client, baseURL+"/menu/get", token, map[string]any{"id": leaf.ID})
	assertEnvelope(t, code, res, http.StatusOK, bizerror.DefaultCode, subcode.MenuNotFound)
	code, res = postJSON(t, client, baseURL+"/menu/list", "", nil)
	assertEnvelope(t, code, res, http.StatusUnauthorized, http.StatusUnauthorized, "common.authentication_required")
	code, res = postJSON(t, client, baseURL+"/auth/menus", "", nil)
	assertEnvelope(t, code, res, http.StatusUnauthorized, http.StatusUnauthorized, "common.authentication_required")
}
