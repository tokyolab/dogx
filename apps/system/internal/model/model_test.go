package model

import "testing"

func TestTableNames(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "user", got: (User{}).TableName(), want: "sys_user"},
		{name: "role", got: (Role{}).TableName(), want: "sys_role"},
		{name: "menu", got: (Menu{}).TableName(), want: "sys_menu"},
		{name: "api", got: (API{}).TableName(), want: "sys_api"},
		{name: "user role", got: (UserRole{}).TableName(), want: "sys_user_role"},
		{name: "role menu", got: (RoleMenu{}).TableName(), want: "sys_role_menu"},
		{name: "login log", got: (LoginLog{}).TableName(), want: "sys_login_log"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("unexpected table name: got %s, want %s", test.got, test.want)
			}
		})
	}
}

func TestMenuConstantsMatchPersistedValues(t *testing.T) {
	if MenuAppAdminWeb != "admin_web" {
		t.Fatalf("unexpected admin web app code: %q", MenuAppAdminWeb)
	}
	if MenuAppAdminMobile != "admin_mobile" {
		t.Fatalf("unexpected admin mobile app code: %q", MenuAppAdminMobile)
	}
	if MenuTypeDirectory != 1 || MenuTypePage != 2 || MenuTypeElement != 3 {
		t.Fatalf(
			"unexpected menu type values: directory=%d page=%d element=%d",
			MenuTypeDirectory,
			MenuTypePage,
			MenuTypeElement,
		)
	}
}
