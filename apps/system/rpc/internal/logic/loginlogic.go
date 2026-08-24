package logic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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
	maxUsernameBytes  = 64
	maxPasswordBytes  = authn.MaxPasswordBytes
	maxIPAddressRunes = 45
	maxUserAgentRunes = 512
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
		l.recordLogin(nil, username, false, model.LoginFailureInvalidCredentials, in)
		return nil, status.Error(codes.InvalidArgument, "invalid login request")
	}

	user, err := l.svcCtx.UserRepo.FindByUsername(l.ctx, username)
	if errors.Is(err, repository.ErrUserNotFound) {
		_ = l.svcCtx.Passwords.Verify(authn.DummyPasswordHash(), in.Password)
		l.recordLogin(nil, username, false, model.LoginFailureInvalidCredentials, in)
		return nil, invalidCredentialsError()
	}
	if err != nil {
		l.recordLogin(nil, username, false, model.LoginFailureSystemError, in)
		return nil, fmt.Errorf("find login user: %w", err)
	}

	if err := l.svcCtx.Passwords.Verify(user.PasswordHash, in.Password); err != nil {
		if errors.Is(err, authn.ErrPasswordMismatch) {
			l.recordLogin(&user.ID, username, false, model.LoginFailureInvalidCredentials, in)
			return nil, invalidCredentialsError()
		}
		l.recordLogin(&user.ID, username, false, model.LoginFailureSystemError, in)
		return nil, fmt.Errorf("verify login password: %w", err)
	}
	if user.Status != model.RecordStatusEnabled {
		l.recordLogin(&user.ID, username, false, model.LoginFailureAccountDisabled, in)
		return nil, bizerror.New("账号已停用")
	}

	credentials, err := l.svcCtx.Tokens.Issue(l.ctx, user.ID)
	if err != nil {
		l.recordLogin(&user.ID, username, false, model.LoginFailureSystemError, in)
		return nil, fmt.Errorf("issue login credentials: %w", err)
	}

	loginAt := time.Now().UTC()
	if err := l.svcCtx.UserRepo.UpdateLastLoginAt(l.ctx, user.ID, loginAt); err != nil {
		l.Errorf("update successful login time: %v", err)
	}
	l.recordLogin(&user.ID, username, true, "", in)

	return &system.LoginResponse{
		AccessToken:  credentials.AccessToken,
		RefreshToken: credentials.RefreshToken,
		ExpiresIn:    credentials.ExpiresIn,
	}, nil
}

func (l *LoginLogic) recordLogin(
	userID *int64,
	username string,
	success bool,
	failureReason string,
	in *system.LoginRequest,
) {
	if l.svcCtx.LoginLogRepo == nil {
		return
	}

	ipAddress := ""
	userAgent := ""
	if in != nil {
		ipAddress = truncateRunes(strings.TrimSpace(in.IpAddress), maxIPAddressRunes)
		userAgent = truncateRunes(strings.TrimSpace(in.UserAgent), maxUserAgentRunes)
	}
	loginLog := &model.LoginLog{
		UserID:        userID,
		Username:      truncateRunes(username, maxUsernameBytes),
		Success:       success,
		FailureReason: failureReason,
		IPAddress:     ipAddress,
		UserAgent:     userAgent,
	}
	if err := l.svcCtx.LoginLogRepo.Create(l.ctx, loginLog); err != nil {
		l.Errorf("record login audit: %v", err)
	}
}

func truncateRunes(value string, maximum int) string {
	runes := []rune(value)
	if len(runes) <= maximum {
		return value
	}
	return string(runes[:maximum])
}

func invalidCredentialsError() error {
	return bizerror.New("用户名或密码错误")
}
