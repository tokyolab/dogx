package logic

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/internal/subcode"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func userPageOffset(page, pageSize int64, keyword string) (int, error) {
	if page <= 0 || pageSize <= 0 || pageSize > 200 || utf8.RuneCountInString(keyword) > 128 ||
		uint64(page-1) > uint64(^uint(0)>>1)/uint64(pageSize) {
		return 0, status.Error(codes.InvalidArgument, "invalid user pagination")
	}
	return int((page - 1) * pageSize), nil
}

func normalizeUserProfile(nickname, email, phone, remark string) (repository.UserProfileUpdate, error) {
	nickname, email, phone, remark = strings.TrimSpace(nickname), strings.TrimSpace(email), strings.TrimSpace(phone), strings.TrimSpace(remark)
	if nickname == "" || utf8.RuneCountInString(nickname) > 64 || utf8.RuneCountInString(email) > 255 ||
		utf8.RuneCountInString(phone) > 32 || utf8.RuneCountInString(remark) > 500 {
		return repository.UserProfileUpdate{}, status.Error(codes.InvalidArgument, "invalid user profile")
	}
	if email != "" {
		address, err := mail.ParseAddress(email)
		if err != nil || address.Address != email {
			return repository.UserProfileUpdate{}, status.Error(codes.InvalidArgument, "invalid user email")
		}
	}
	return repository.UserProfileUpdate{Nickname: nickname, Email: optionalContact(email), Phone: optionalContact(phone), Remark: remark}, nil
}

func optionalContact(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func validUserRoleIDs(ids []int64) bool {
	return len(ids) <= 100 && !slices.ContainsFunc(ids, func(id int64) bool { return id <= 0 })
}

func userHasSuperRole(record *repository.UserRecord) bool {
	return slices.ContainsFunc(record.Roles, func(role model.Role) bool { return role.Code == model.SuperAdminRoleCode })
}

func managedUser(ctx context.Context, sc *svc.ServiceContext, operatorID, userID int64, deactivating bool) (*repository.UserRecord, error) {
	if operatorID <= 0 || userID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid user management identity")
	}
	if deactivating && operatorID == userID {
		return nil, bizerror.New(subcode.UserSelfProtected, "不能停用或删除当前登录账号")
	}
	record, err := sc.UserRepo.FindWithRoles(ctx, userID)
	if err != nil {
		return nil, userManagementError(err)
	}
	if record == nil {
		return nil, status.Error(codes.Internal, "user repository returned no user")
	}
	// There is only one initialized super administrator. Its profile and
	// credentials are self-managed; no operator may disable or delete it.
	// The API obtains operatorID from authentication, never the HTTP body.
	if userHasSuperRole(record) && (deactivating || operatorID != userID) {
		return nil, userManagementError(repository.ErrSuperAdminProtected)
	}
	return record, nil
}

func toUserRoleInfo(role model.Role) *system.UserRoleInfo {
	return &system.UserRoleInfo{Id: role.ID, Code: role.Code, Name: role.Name, Status: int32(role.Status)}
}

func toUserInfo(record repository.UserRecord) *system.UserInfo {
	user := record.User
	roles := make([]*system.UserRoleInfo, 0, len(record.Roles))
	for _, role := range record.Roles {
		roles = append(roles, toUserRoleInfo(role))
	}
	info := &system.UserInfo{Id: user.ID, Username: user.Username, Nickname: user.Nickname,
		Remark: user.Remark, Status: int32(user.Status), Roles: roles,
		CreatedAt: user.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: user.UpdatedAt.UTC().Format(time.RFC3339Nano)}
	if user.Email != nil {
		info.Email = *user.Email
	}
	if user.Phone != nil {
		info.Phone = *user.Phone
	}
	if user.LastLoginAt != nil {
		info.LastLoginAt = user.LastLoginAt.UTC().Format(time.RFC3339Nano)
	}
	return info
}

func userManagementError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, repository.ErrUserNotFound):
		return bizerror.New(subcode.UserNotFound, "用户不存在")
	case errors.Is(err, repository.ErrUsernameExists):
		return bizerror.New(subcode.UserUsernameExists, "用户名已存在")
	case errors.Is(err, repository.ErrUserEmailExists):
		return bizerror.New(subcode.UserEmailExists, "邮箱已被使用")
	case errors.Is(err, repository.ErrUserPhoneExists):
		return bizerror.New(subcode.UserPhoneExists, "手机号已被使用")
	case errors.Is(err, repository.ErrSuperAdminNotAssignable):
		return bizerror.New(subcode.UserSuperAdminNotAssignable, "超管角色不允许授权")
	case errors.Is(err, repository.ErrUserRoleUnavailable):
		return bizerror.New(subcode.UserRoleUnavailable, "角色不存在或不允许分配")
	case errors.Is(err, repository.ErrSuperAdminProtected):
		return bizerror.New(subcode.UserSuperAdminProtected, "初始化超管账号受保护，不能停用、删除、修改角色或由其他账号修改")
	default:
		return fmt.Errorf("manage user: %w", err)
	}
}
