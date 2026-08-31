package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrRoleNotFound        = errors.New("role not found")
	ErrRoleCodeExists      = errors.New("role code already exists")
	ErrReservedRoleCode    = errors.New("role code is reserved by the system")
	ErrRoleInUse           = errors.New("role is assigned to users")
	ErrSystemRoleProtected = errors.New("system role is protected")
)

type RoleListQuery struct {
	Keyword string
	Offset  int
	Limit   int
}

type RoleRepository interface {
	ListEnabledRoleIDs(ctx context.Context, userID int64) ([]int64, error)
	IsSuperAdmin(ctx context.Context, userID int64) (bool, error)
	List(ctx context.Context, query RoleListQuery) ([]model.Role, int64, error)
	FindByID(ctx context.Context, id int64) (*model.Role, error)
}

type RoleUpdate struct {
	Code        string
	Name        string
	Description string
	Sort        int32
}

type RoleWriter interface {
	Create(ctx context.Context, role *model.Role) error
	Update(ctx context.Context, id int64, update RoleUpdate) error
}

type RoleStore interface {
	RoleRepository
	RoleWriter
}

type roleRepository struct {
	db *gorm.DB
}

func NewRoleRepository(db *gorm.DB) (RoleStore, error) {
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

func (r *roleRepository) IsSuperAdmin(ctx context.Context, userID int64) (bool, error) {
	if ctx == nil {
		return false, errors.New("check super administrator context is nil")
	}
	if userID <= 0 {
		return false, errors.New("user id must be positive")
	}

	var count int64
	if err := r.db.WithContext(ctx).
		Model(&model.Role{}).
		Joins("JOIN sys_user_role ON sys_user_role.role_id = sys_role.id").
		Where(
			"sys_user_role.user_id = ? AND sys_role.code = ? AND sys_role.status = ?",
			userID,
			model.SuperAdminRoleCode,
			model.RecordStatusEnabled,
		).
		Limit(1).
		Count(&count).Error; err != nil {
		return false, fmt.Errorf("check super administrator: %w", err)
	}
	return count > 0, nil
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

func (r *roleRepository) Create(ctx context.Context, role *model.Role) error {
	if ctx == nil {
		return errors.New("create role context is nil")
	}
	if role == nil {
		return errors.New("role is nil")
	}
	if role.Code == model.SuperAdminRoleCode {
		return ErrReservedRoleCode
	}
	if err := r.db.WithContext(ctx).Create(role).Error; err != nil {
		return mapRoleWriteError("create role", err)
	}
	return nil
}

func (r *roleRepository) Update(ctx context.Context, id int64, update RoleUpdate) error {
	if ctx == nil {
		return errors.New("update role context is nil")
	}
	if id <= 0 {
		return errors.New("role id must be positive")
	}

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var role model.Role
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&role, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRoleNotFound
			}
			return fmt.Errorf("lock role for update: %w", err)
		}
		if role.IsSystem && update.Code != role.Code {
			return ErrSystemRoleProtected
		}
		if update.Code == model.SuperAdminRoleCode && role.Code != model.SuperAdminRoleCode {
			return ErrReservedRoleCode
		}

		result := tx.Model(&role).Updates(map[string]any{
			"code":        update.Code,
			"name":        update.Name,
			"description": update.Description,
			"sort":        update.Sort,
		})
		if result.Error != nil {
			return mapRoleWriteError("update role", result.Error)
		}
		if result.RowsAffected == 0 {
			return ErrRoleNotFound
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func mapRoleWriteError(operation string, err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch {
		case postgresError.Code == "23505" &&
			postgresError.ConstraintName == "uk_sys_role_code_active":
			return ErrRoleCodeExists
		case postgresError.Code == "23514" &&
			postgresError.ConstraintName == "ck_sys_role_super_admin_system":
			return ErrReservedRoleCode
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}

var _ RoleRepository = (*roleRepository)(nil)
var _ RoleWriter = (*roleRepository)(nil)
