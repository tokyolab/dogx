package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

type RoleRepository interface {
	ListEnabledRoleIDs(ctx context.Context, userID int64) ([]int64, error)
}

type roleRepository struct {
	db *gorm.DB
}

func NewRoleRepository(db *gorm.DB) (RoleRepository, error) {
	if db == nil {
		return nil, errors.New("role repository database is nil")
	}
	return &roleRepository{db: db}, nil
}

func (r *roleRepository) ListEnabledRoleIDs(ctx context.Context, userID int64) ([]int64, error) {
	if ctx == nil {
		return nil, errors.New("list user roles context is nil")
	}
	if userID <= 0 {
		return nil, errors.New("user id must be positive")
	}

	var roleIDs []int64
	if err := r.db.WithContext(ctx).
		Model(&model.Role{}).
		Distinct("sys_role.id").
		Joins("JOIN sys_user_role ON sys_user_role.role_id = sys_role.id").
		Where("sys_user_role.user_id = ? AND sys_role.status = ?", userID, model.RecordStatusEnabled).
		Order("sys_role.id").
		Pluck("sys_role.id", &roleIDs).Error; err != nil {
		return nil, fmt.Errorf("list enabled user roles: %w", err)
	}
	return roleIDs, nil
}
