package authn

import (
	"strings"
	"testing"
)

func TestValidateUsername(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"empty", "", false},
		{"single letter", "A", true},
		{"single digit", "0", true},
		{"mixed case", "DogX-Admin123", true},
		{"digits only", "123456", true},
		{"separate hyphens", "a-b-c", true},
		{"64 characters", strings.Repeat("A", 64), true},
		{"65 characters", strings.Repeat("a", 65), false},
		{"hyphen only", "-", false},
		{"leading hyphen", "-admin", false},
		{"trailing hyphen", "admin-", false},
		{"consecutive hyphens", "ad--min", false},
		{"underscore", "admin_01", false},
		{"period", "admin.01", false},
		{"Chinese", "管理员", false},
		{"accented letter", "café", false},
		{"full width", "Ａdmin", false},
		{"Unicode hyphen", "a–b", false},
		{"emoji", "admin😀", false},
		{"leading space", " admin", false},
		{"trailing space", "admin ", false},
		{"internal space", "ad min", false},
		{"tab", "ad\tmin", false},
		{"newline", "admin\n", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateUsername(tt.value); (err == nil) != tt.valid {
				t.Fatalf("ValidateUsername(%q) = %v, want valid=%t", tt.value, err, tt.valid)
			}
		})
	}
}
