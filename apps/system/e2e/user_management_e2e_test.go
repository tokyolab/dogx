//go:build integration && e2e

package e2e

import (
	"net/http"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/pkg/bizerror"
	commonsubcode "github.com/tokyolab/dogx/pkg/subcode"
)

func testUserManagement(t *testing.T, client *http.Client, baseURL, adminToken string, adminID, superRoleID int64, process *runningProcess, track func(int64, string)) {
	t.Helper()
	call := func(path string, body any) responseEnvelope {
		t.Helper()
		code, envelope := postJSON(t, client, baseURL+path, adminToken, body)
		assertEnvelope(t, code, envelope, http.StatusOK, 0, "")
		return envelope
	}
	roleResponse := call("/role/create", map[string]any{"code": "user_management_reader", "name": "User reader", "sort": 0, "status": 1})
	var role createdRoleData
	decodeData(t, roleResponse, &role)
	// Even the super administrator cannot assign roles to itself; rejection
	// must preserve its session so it can continue administering other users.
	for _, roleIDs := range [][]int64{{role.ID}, {superRoleID}, {}} {
		code, envelope := postJSON(t, client, baseURL+"/user/role/update", adminToken, map[string]any{"id": adminID, "roleIds": roleIDs})
		assertEnvelope(t, code, envelope, http.StatusOK, bizerror.DefaultCode, subcode.UserSuperAdminProtected)
		call("/auth/me", nil)
	}
	apiResponse := call("/api/list", map[string]any{})
	var apis struct {
		Items []struct {
			ID   int64  `json:"id"`
			Path string `json:"path"`
		} `json:"items"`
	}
	decodeData(t, apiResponse, &apis)
	ids := make([]int64, 0, 3)
	for _, api := range apis.Items {
		if api.Path == "/role/list" || api.Path == "/user/password/reset" || api.Path == "/user/role/update" {
			ids = append(ids, api.ID)
		}
	}
	if len(ids) != 3 {
		t.Fatalf("user management API resources missing: %v", ids)
	}
	call("/role/api/update", map[string]any{"roleId": role.ID, "apiIds": ids})
	created := call("/user/create", map[string]any{"username": "managed-user", "nickname": "Managed User", "password": e2ePassword, "email": "managed@example.com", "phone": "123456", "remark": "original", "status": 1, "roleIds": []int64{role.ID}})
	var user struct {
		ID int64 `json:"id"`
	}
	decodeData(t, created, &user)
	track(user.ID, "")
	login := func(password string) loginData {
		t.Helper()
		code, envelope := postJSON(t, client, baseURL+"/auth/login", "", map[string]any{"username": "managed-user", "password": password})
		assertEnvelope(t, code, envelope, http.StatusOK, 0, "")
		var credentials loginData
		decodeData(t, envelope, &credentials)
		track(user.ID, credentials.RefreshToken)
		return credentials
	}
	credentials := login(e2ePassword)
	waitForPermissionGrant(t, client, baseURL+"/role/list", credentials.AccessToken, map[string]any{"page": 1, "pageSize": 20}, process)
	code, envelope := postJSON(t, client, baseURL+"/user/password/reset", credentials.AccessToken, map[string]any{"id": adminID, "operatorId": adminID, "password": "different-password"})
	assertEnvelope(t, code, envelope, http.StatusOK, bizerror.DefaultCode, subcode.UserSuperAdminProtected)
	code, envelope = postJSON(t, client, baseURL+"/user/role/update", credentials.AccessToken, map[string]any{"id": user.ID, "roleIds": []int64{superRoleID}})
	assertEnvelope(t, code, envelope, http.StatusOK, bizerror.DefaultCode, subcode.UserSuperAdminNotAssignable)
	code, envelope = postJSON(t, client, baseURL+"/auth/me", credentials.AccessToken, nil)
	assertEnvelope(t, code, envelope, http.StatusOK, 0, "")
	// Repeating the same role set through HTTP must preserve the user's session.
	call("/user/role/update", map[string]any{"id": user.ID, "roleIds": []int64{role.ID, role.ID}})
	code, envelope = postJSON(t, client, baseURL+"/auth/me", credentials.AccessToken, nil)
	assertEnvelope(t, code, envelope, http.StatusOK, 0, "")
	call("/user/update", map[string]any{"id": user.ID, "nickname": "新昵称", "email": "", "phone": "", "remark": ""})
	detail := call("/user/get", map[string]any{"id": user.ID})
	var profile struct {
		Username string `json:"username"`
		Nickname string `json:"nickname"`
		Email    string `json:"email"`
		Phone    string `json:"phone"`
		Remark   string `json:"remark"`
		Roles    []struct {
			ID int64 `json:"id"`
		} `json:"roles"`
	}
	decodeData(t, detail, &profile)
	if profile.Username != "managed-user" || profile.Nickname != "新昵称" || profile.Email != "" || profile.Phone != "" || profile.Remark != "" || len(profile.Roles) != 1 {
		t.Fatalf("profile update changed unrelated fields: %+v", profile)
	}
	options := call("/user/role/options", map[string]any{"page": 1, "pageSize": 200})
	var selectable struct {
		Items []struct {
			Code   string `json:"code"`
			Status int32  `json:"status"`
		} `json:"items"`
	}
	decodeData(t, options, &selectable)
	for _, item := range selectable.Items {
		if item.Code == "super_admin" || item.Status != 1 {
			t.Fatalf("unassignable role exposed: %+v", item)
		}
	}
	call("/user/role/update", map[string]any{"id": user.ID, "roleIds": []int64{}})
	code, envelope = postJSON(t, client, baseURL+"/auth/me", credentials.AccessToken, nil)
	assertEnvelope(t, code, envelope, http.StatusUnauthorized, http.StatusUnauthorized, commonsubcode.AuthenticationRequired)
	credentials = login(e2ePassword)
	code, envelope = postJSON(t, client, baseURL+"/role/list", credentials.AccessToken, map[string]any{"page": 1, "pageSize": 20})
	assertEnvelope(t, code, envelope, http.StatusForbidden, http.StatusForbidden, commonsubcode.PermissionDenied)
	call("/user/password/reset", map[string]any{"id": user.ID, "password": "new-managed-password"})
	code, envelope = postJSON(t, client, baseURL+"/auth/me", credentials.AccessToken, nil)
	assertEnvelope(t, code, envelope, http.StatusUnauthorized, http.StatusUnauthorized, commonsubcode.AuthenticationRequired)
	code, envelope = postJSON(t, client, baseURL+"/auth/login", "", map[string]any{"username": "managed-user", "password": e2ePassword})
	assertEnvelope(t, code, envelope, http.StatusOK, bizerror.DefaultCode, subcode.AuthInvalidCredentials)
	credentials = login("new-managed-password")
	call("/user/status/update", map[string]any{"id": user.ID, "status": 0})
	code, envelope = postJSON(t, client, baseURL+"/auth/me", credentials.AccessToken, nil)
	assertEnvelope(t, code, envelope, http.StatusUnauthorized, http.StatusUnauthorized, commonsubcode.AuthenticationRequired)
	code, envelope = postJSON(t, client, baseURL+"/auth/login", "", map[string]any{"username": "managed-user", "password": "new-managed-password"})
	assertEnvelope(t, code, envelope, http.StatusOK, bizerror.DefaultCode, subcode.AuthUserDisabled)
	call("/user/status/update", map[string]any{"id": user.ID, "status": 1})
	call("/user/delete", map[string]any{"id": user.ID})
	code, envelope = postJSON(t, client, baseURL+"/user/get", adminToken, map[string]any{"id": user.ID})
	assertEnvelope(t, code, envelope, http.StatusOK, bizerror.DefaultCode, subcode.UserNotFound)
	call("/role/delete", map[string]any{"id": role.ID})
}
