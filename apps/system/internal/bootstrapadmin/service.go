package bootstrapadmin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/system/internal/authn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

const (
	maxUsernameCharacters = 64
	maxNicknameCharacters = 64
	InitialRoleCode       = model.SuperAdminRoleCode
)

type Input struct {
	Username string
	Password string
	Nickname string
}

type gormUserCreator struct {
	db *gorm.DB
}

func (c gormUserCreator) Create(ctx context.Context, user *model.User) error {
	if err := c.db.WithContext(ctx).Create(user).Error; err != nil {
		return fmt.Errorf("create user: %w", err)
	}
	return nil
}

func CreateInitialAdministrator(
	ctx context.Context,
	db *gorm.DB,
	hasher authn.PasswordHasher,
	input Input,
) (*model.User, error) {
	if ctx == nil {
		return nil, errors.New("bootstrap administrator context is nil")
	}
	if db == nil {
		return nil, errors.New("bootstrap administrator database is nil")
	}

	var user *model.User
	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var role model.Role
		if err := tx.
			Where("code = ? AND status = ?", InitialRoleCode, model.RecordStatusEnabled).
			First(&role).Error; err != nil {
			return fmt.Errorf("load initial administrator role: %w", err)
		}

		created, err := Create(ctx, gormUserCreator{db: tx}, hasher, input)
		if err != nil {
			return err
		}
		if err := tx.WithContext(ctx).Create(&model.UserRole{
			UserID: created.ID,
			RoleID: role.ID,
		}).Error; err != nil {
			return fmt.Errorf("assign initial administrator role: %w", err)
		}
		user = created
		return nil
	}); err != nil {
		return nil, err
	}
	return user, nil
}

type userCreator interface {
	Create(ctx context.Context, user *model.User) error
}

func Create(
	ctx context.Context,
	repo userCreator,
	hasher authn.PasswordHasher,
	input Input,
) (*model.User, error) {
	if repo == nil {
		return nil, errors.New("user repository is nil")
	}
	if hasher == nil {
		return nil, errors.New("password hasher is nil")
	}

	username := strings.TrimSpace(input.Username)
	nickname := strings.TrimSpace(input.Nickname)
	if nickname == "" {
		nickname = username
	}
	if username == "" || utf8.RuneCountInString(username) > maxUsernameCharacters {
		return nil, fmt.Errorf("username must contain 1 to %d characters", maxUsernameCharacters)
	}
	if nickname == "" || utf8.RuneCountInString(nickname) > maxNicknameCharacters {
		return nil, fmt.Errorf("nickname must contain 1 to %d characters", maxNicknameCharacters)
	}
	if err := authn.ValidatePassword(input.Password); err != nil {
		return nil, err
	}

	passwordHash, err := hasher.Hash(input.Password)
	if err != nil {
		return nil, fmt.Errorf("hash administrator password: %w", err)
	}
	user := &model.User{
		Username:     username,
		PasswordHash: passwordHash,
		Nickname:     nickname,
		Status:       model.RecordStatusEnabled,
		Remark:       "initial administrator",
	}
	if err := repo.Create(ctx, user); err != nil {
		return nil, err
	}

	return user, nil
}
