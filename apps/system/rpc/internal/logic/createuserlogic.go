package logic

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateUserLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateUserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateUserLogic {
	return &CreateUserLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateUserLogic) CreateUser(in *system.CreateUserRequest) (*system.CreateUserResponse, error) {
	if in == nil || !validRecordStatus(in.Status) || !validUserRoleIDs(in.RoleIds) {
		return nil, status.Error(codes.InvalidArgument, "invalid create user request")
	}
	username := strings.TrimSpace(in.Username)
	if username == "" || utf8.RuneCountInString(username) > 64 || authn.ValidatePassword(in.Password) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user credentials")
	}
	profile, err := normalizeUserProfile(in.Nickname, in.Email, in.Phone, in.Remark)
	if err != nil {
		return nil, err
	}
	hash, err := l.svcCtx.Passwords.Hash(in.Password)
	if err != nil {
		return nil, fmt.Errorf("hash initial user password: %w", err)
	}
	user := &model.User{Username: username, PasswordHash: hash, Nickname: profile.Nickname, Email: profile.Email, Phone: profile.Phone, Remark: profile.Remark, Status: model.RecordStatus(in.Status)}
	if err := l.svcCtx.UserRepo.CreateWithRoles(l.ctx, user, in.RoleIds); err != nil {
		return nil, userManagementError(err)
	}
	return &system.CreateUserResponse{Id: user.ID}, nil
}
