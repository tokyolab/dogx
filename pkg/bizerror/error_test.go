package bizerror

import (
	"fmt"
	"testing"
)

func TestNewUsesDefaultCodeAndPreservesSubcode(t *testing.T) {
	err := New("system.user.username_exists", "username already exists")

	if err.Code() != DefaultCode {
		t.Fatalf("unexpected code: %d", err.Code())
	}
	if err.Subcode() != "system.user.username_exists" {
		t.Fatalf("unexpected subcode: %s", err.Subcode())
	}
	if err.Error() != "username already exists" {
		t.Fatalf("unexpected message: %s", err.Error())
	}
}

func TestFromWrappedError(t *testing.T) {
	want := NewCode(100002, "system.auth.token_expired", "token expired")
	got, ok := From(fmt.Errorf("register user: %w", want))

	if !ok {
		t.Fatal("expected business error")
	}
	if got != want {
		t.Fatalf("unexpected business error: %#v", got)
	}
}

func TestIsCode(t *testing.T) {
	tests := []struct {
		name string
		code uint32
		want bool
	}{
		{name: "below range", code: MinCode - 1, want: false},
		{name: "minimum", code: MinCode, want: true},
		{name: "default", code: DefaultCode, want: true},
		{name: "maximum", code: MaxCode, want: true},
		{name: "above range", code: MaxCode + 1, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsCode(tt.code); got != tt.want {
				t.Fatalf("IsCode(%d) = %t, want %t", tt.code, got, tt.want)
			}
		})
	}
}

func TestIsSubcode(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{value: "system.role.not_found", want: true},
		{value: "common.invalid_request", want: true},
		{value: "", want: false},
		{value: "ROLE_NOT_FOUND", want: false},
		{value: "system", want: false},
		{value: "system.role.not-found", want: false},
	}

	for _, test := range tests {
		if got := IsSubcode(test.value); got != test.want {
			t.Errorf("IsSubcode(%q) = %t, want %t", test.value, got, test.want)
		}
	}
}
