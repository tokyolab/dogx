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
		{name: "user role", got: (UserRole{}).TableName(), want: "sys_user_role"},
		{name: "role menu", got: (RoleMenu{}).TableName(), want: "sys_role_menu"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("unexpected table name: got %s, want %s", test.got, test.want)
			}
		})
	}
}
