package authctx

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestFromContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), userIDClaimKey, json.Number("42"))
	ctx = context.WithValue(ctx, sessionIDClaimKey, "session-id")
	ctx = context.WithValue(ctx, roleIDsClaimKey, []any{json.Number("2"), float64(7), float64(7)})
	ctx = context.WithValue(ctx, superAdminClaimKey, true)

	identity, err := FromContext(ctx)
	if err != nil {
		t.Fatalf("extract identity: %v", err)
	}
	if identity.UserID != 42 || identity.SessionID != "session-id" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
	if len(identity.RoleIDs) != 2 || identity.RoleIDs[0] != 2 || identity.RoleIDs[1] != 7 {
		t.Fatalf("unexpected role identity: %+v", identity)
	}
	if !identity.IsSuperAdmin {
		t.Fatal("super administrator claim was not extracted")
	}
}

func TestFromContextRejectsInvalidRoleClaims(t *testing.T) {
	for _, roleIDs := range []any{
		"1",
		[]any{float64(1.5)},
		[]any{json.Number("0")},
		[]any{json.Number("invalid")},
	} {
		ctx := context.WithValue(context.Background(), userIDClaimKey, json.Number("42"))
		ctx = context.WithValue(ctx, sessionIDClaimKey, "session-id")
		ctx = context.WithValue(ctx, roleIDsClaimKey, roleIDs)
		ctx = context.WithValue(ctx, superAdminClaimKey, false)
		if _, err := FromContext(ctx); !errors.Is(err, ErrInvalidIdentity) {
			t.Fatalf("expected role claims %v to be rejected, got: %v", roleIDs, err)
		}
	}
}

func TestFromContextRejectsInvalidClaims(t *testing.T) {
	missingSuperAdmin := context.WithValue(context.Background(), userIDClaimKey, json.Number("42"))
	missingSuperAdmin = context.WithValue(missingSuperAdmin, sessionIDClaimKey, "session-id")
	missingSuperAdmin = context.WithValue(missingSuperAdmin, roleIDsClaimKey, []any{})
	invalidSuperAdmin := context.WithValue(missingSuperAdmin, superAdminClaimKey, "true")
	tests := []context.Context{
		nil,
		context.Background(),
		context.WithValue(context.Background(), userIDClaimKey, json.Number("invalid")),
		context.WithValue(context.Background(), userIDClaimKey, json.Number("0")),
		context.WithValue(context.Background(), userIDClaimKey, 42),
		missingSuperAdmin,
		invalidSuperAdmin,
	}
	for _, ctx := range tests {
		if _, err := FromContext(ctx); !errors.Is(err, ErrInvalidIdentity) {
			t.Fatalf("expected invalid identity error, got: %v", err)
		}
	}
}
