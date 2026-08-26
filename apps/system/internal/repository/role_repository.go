package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

var ErrRoleNotFound = errors.New("role not found")

type RoleListQuery struct {
	Keyword string
	Offset  int
	Limit   int
}

type RoleRepository interface {
	ListEnabledRoleIDs(ctx context.Context, userID int64) ([]int64, error)
	List(ctx context.Context, query RoleListQuery) ([]model.Role, int64, error)
	FindByID(ctx context.Context, id int64) (*model.Role, error)
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

func (r *roleRepository) List(
	ctx context.Context,
	query RoleListQuery,
) ([]model.Role, int64, error) {
	if ctx == nil {
		return nil, 0, errors.New("list roles context is nil")
	}
	if query.Offset < 0 || query.Limit <= 0 {
		return nil, 0, errors.New("invalid role list pagination")
	}

	database := r.db.WithContext(ctx).Model(&model.Role{})
	if keyword := strings.TrimSpace(query.Keyword); keyword != "" {
		pattern := containsLikePattern(keyword)
		database = database.Where(
			"(code ILIKE ? ESCAPE '!' OR name ILIKE ? ESCAPE '!')",
			pattern,
			pattern,
		)
	}

	var total int64
	if err := database.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count roles: %w", err)
	}

	roles := make([]model.Role, 0)
	if err := database.
		Order("sort ASC, id ASC").
		Offset(query.Offset).
		Limit(query.Limit).
		Find(&roles).Error; err != nil {
		return nil, 0, fmt.Errorf("list roles: %w", err)
	}
	return roles, total, nil
}

func (r *roleRepository) FindByID(ctx context.Context, id int64) (*model.Role, error) {
	if ctx == nil {
		return nil, errors.New("find role context is nil")
	}
	if id <= 0 {
		return nil, errors.New("role id must be positive")
	}

	var role model.Role
	if err := r.db.WithContext(ctx).First(&role, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrRoleNotFound
		}
		return nil, fmt.Errorf("find role: %w", err)
	}
	return &role, nil
}
