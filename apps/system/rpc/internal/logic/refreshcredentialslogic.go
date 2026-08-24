package logic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RefreshCredentialsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewRefreshCredentialsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshCredentialsLogic {
	return &RefreshCredentialsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *RefreshCredentialsLogic) RefreshCredentials(in *system.RefreshCredentialsRequest) (*system.LoginResponse, error) {
	if in == nil || strings.TrimSpace(in.RefreshToken) == "" || len(in.RefreshToken) > 256 {
		return nil, status.Error(codes.InvalidArgument, "invalid refresh request")
	}

	credentials, err := l.svcCtx.RefreshTokens.Refresh(l.ctx, in.RefreshToken)
	if errors.Is(err, authn.ErrInvalidRefreshToken) {
		return nil, status.Error(codes.Unauthenticated, "refresh token is invalid or expired")
	}
	if err != nil {
		return nil, fmt.Errorf("refresh credentials: %w", err)
	}

	return &system.LoginResponse{
		AccessToken:  credentials.AccessToken,
		RefreshToken: credentials.RefreshToken,
		ExpiresIn:    credentials.ExpiresIn,
	}, nil
}
