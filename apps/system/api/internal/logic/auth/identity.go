package auth

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/authctx"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func authenticatedIdentity(ctx context.Context) (authctx.Identity, error) {
	identity, err := authctx.FromContext(ctx)
	if err != nil {
		return authctx.Identity{}, status.Error(codes.Unauthenticated, "authentication required")
	}
	return identity, nil
}
