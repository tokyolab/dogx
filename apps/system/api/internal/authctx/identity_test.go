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

	identity, err := FromContext(ctx)
	if err != nil {
		t.Fatalf("extract identity: %v", err)
	}
	if identity.UserID != 42 || identity.SessionID != "session-id" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}

func TestFromContextRejectsInvalidClaims(t *testing.T) {
	tests := []context.Context{
		nil,
		context.Background(),
		context.WithValue(context.Background(), userIDClaimKey, json.Number("invalid")),
		context.WithValue(context.Background(), userIDClaimKey, json.Number("0")),
		context.WithValue(context.Background(), userIDClaimKey, 42),
	}
	for _, ctx := range tests {
		if _, err := FromContext(ctx); !errors.Is(err, ErrInvalidIdentity) {
			t.Fatalf("expected invalid identity error, got: %v", err)
		}
	}
}
