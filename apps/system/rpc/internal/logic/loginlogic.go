package logic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxUsernameBytes = 64
	maxPasswordBytes = 128
)

type LoginLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LoginLogic) Login(in *system.LoginRequest) (*system.LoginResponse, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid login request")
	}

	username := strings.TrimSpace(in.Username)
	if username == "" || len(username) > maxUsernameBytes ||
		in.Password == "" || len(in.Password) > maxPasswordBytes {
		return nil, status.Error(codes.InvalidArgument, "invalid login request")
	}

	user, err := l.svcCtx.UserRepo.FindByUsername(l.ctx, username)
	if errors.Is(err, repository.ErrUserNotFound) {
		_ = l.svcCtx.Passwords.Verify(authn.DummyPasswordHash(), in.Password)
		return nil, invalidCredentialsError()
	}
	if err != nil {
		return nil, fmt.Errorf("find login user: %w", err)
	}

	if err := l.svcCtx.Passwords.Verify(user.PasswordHash, in.Password); err != nil {
		if errors.Is(err, authn.ErrPasswordMismatch) {
			return nil, invalidCredentialsError()
		}
		return nil, fmt.Errorf("verify login password: %w", err)
	}
	if user.Status != model.RecordStatusEnabled {
		return nil, bizerror.New("账号已停用")
	}

	credentials, err := l.svcCtx.Tokens.Issue(l.ctx, user.ID)
	if err != nil {
		return nil, fmt.Errorf("issue login credentials: %w", err)
	}

	return &system.LoginResponse{
		AccessToken:  credentials.AccessToken,
		RefreshToken: credentials.RefreshToken,
		ExpiresIn:    credentials.ExpiresIn,
	}, nil
}

func invalidCredentialsError() error {
	return bizerror.New("用户名或密码错误")
}
