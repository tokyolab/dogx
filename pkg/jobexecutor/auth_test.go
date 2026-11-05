package jobexecutor

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These synthetic credentials never identify a deployed executor group.
const (
	adapterTestGroup = "test-workers"
	adapterTestToken = "11111111111141118111111111111111"
	adapterTestID    = "22222222-2222-4222-8222-222222222222"
)

func TestValidateCredentials(t *testing.T) {
	for _, group := range []string{adapterTestGroup, strings.Repeat("界", 128)} {
		if err := ValidateCredentials(group, adapterTestToken); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct{ name, group, token string }{
		{"missing group", "", adapterTestToken},
		{"group whitespace", " test-workers", adapterTestToken},
		{"group NUL", "test\x00workers", adapterTestToken},
		{"group invalid UTF-8", string([]byte{0xff}), adapterTestToken},
		{"group too long", strings.Repeat("界", 129), adapterTestToken},
		{"missing token", adapterTestGroup, ""},
		{"token with hyphens", adapterTestGroup, adapterTestID},
		{"token uppercase", adapterTestGroup, "AAAAAAAAAAAA4AAA8AAAAAAAAAAAAAAA"},
		{"token nonhex", adapterTestGroup, "1111111111114111811111111111111g"},
		{"token wrong version", adapterTestGroup, "11111111111111118111111111111111"},
		{"token wrong variant", adapterTestGroup, "11111111111141117111111111111111"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCredentials(test.group, test.token)
			if err == nil {
				t.Fatal("invalid configuration was accepted")
			}
			if strings.Contains(err.Error(), adapterTestToken) {
				t.Fatal("configuration error exposed a token")
			}
		})
	}
}

func TestAuthenticatorExactCredentials(t *testing.T) {
	auth, err := NewAuthenticator(adapterTestGroup, adapterTestToken)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, group, token string
		code               codes.Code
	}{
		{"valid", adapterTestGroup, adapterTestToken, codes.OK},
		{"no credentials", "", "", codes.Unauthenticated},
		{"missing group", "", adapterTestToken, codes.Unauthenticated},
		{"missing token", adapterTestGroup, "", codes.Unauthenticated},
		{"wrong token", adapterTestGroup, strings.Repeat("3", 32), codes.Unauthenticated},
		{"case differs", "Test-workers", adapterTestToken, codes.PermissionDenied},
		{"group whitespace", adapterTestGroup + " ", adapterTestToken, codes.PermissionDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := auth.Authenticate(test.group, test.token)
			if status.Code(err) != test.code {
				t.Fatalf("code = %v, want %v", status.Code(err), test.code)
			}
			if err != nil && strings.Contains(err.Error(), adapterTestToken) {
				t.Fatal("authentication error exposed a token")
			}
		})
	}
	if _, err := NewAuthenticator(adapterTestGroup, "invalid"); err == nil {
		t.Fatal("invalid authenticator was constructed")
	}
}
