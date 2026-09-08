package repository

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/system/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrUserNotFound            = errors.New("user not found")
	ErrUsernameExists          = errors.New("username already exists")
	ErrUserEmailExists         = errors.New("user email already exists")
	ErrUserPhoneExists         = errors.New("user phone already exists")
	ErrUserRoleUnavailable     = errors.New("role is unavailable for assignment")
	ErrSuperAdminNotAssignable = errors.New("super administrator cannot be assigned")
	ErrSuperAdminProtected     = errors.New("the initialized super administrator account is protected")
)

type UserRecord struct {
	User  model.User
	Roles []model.Role
}

type UserListQuery struct {
	Keyword string
	Status  *model.RecordStatus
	Offset  int
	Limit   int
}

type UserProfileUpdate struct {
	Nickname string
	Email    *string
	Phone    *string
	Remark   string
}

type UserRepository interface {
	Create(ctx context.Context, user *model.User) error
	FindByID(ctx context.Context, id int64) (*model.User, error)
	FindByUsername(ctx context.Context, username string) (*model.User, error)
	UpdateLastLoginAt(ctx context.Context, id int64, lastLoginAt time.Time) error
	UpdatePasswordHash(ctx context.Context, id int64, passwordHash string) error
	List(ctx context.Context, query UserListQuery) ([]UserRecord, int64, error)
	FindWithRoles(ctx context.Context, id int64) (*UserRecord, error)
	CreateWithRoles(ctx context.Context, user *model.User, roleIDs []int64) error
	ValidateRoles(ctx context.Context, roleIDs []int64, current []model.Role) error
	UpdateProfile(ctx context.Context, id int64, update UserProfileUpdate) error
	ReplaceRoles(ctx context.Context, id int64, roleIDs []int64) error
	UpdateStatus(ctx context.Context, id int64, status model.RecordStatus) error
	Delete(ctx context.Context, id int64) error
}

type userRepository struct {
	db *gorm.DB
}

func NewUserRepository(db *gorm.DB) (UserRepository, error) {
	if db == nil {
		return nil, errors.New("user repository database is nil")
	}
	return &userRepository{db: db}, nil
}

func (r *userRepository) Create(ctx context.Context, user *model.User) error {
	if err := r.db.WithContext(ctx).Create(user).Error; err != nil {
		return fmt.Errorf("create user: %w", err)
	}
	return nil
}

func (r *userRepository) FindByID(ctx context.Context, id int64) (*model.User, error) {
	var user model.User
	if err := r.db.WithContext(ctx).First(&user, id).Error; err != nil {
		return nil, mapUserError(err)
	}
	return &user, nil
}

func (r *userRepository) FindByUsername(ctx context.Context, username string) (*model.User, error) {
	var user model.User
	if err := r.db.WithContext(ctx).
		Where("LOWER(username) = LOWER(?)", username).
		First(&user).Error; err != nil {
		return nil, mapUserError(err)
	}
	return &user, nil
}

func (r *userRepository) UpdateLastLoginAt(ctx context.Context, id int64, lastLoginAt time.Time) error {
	result := r.db.WithContext(ctx).
		Model(&model.User{}).
		Where("id = ?", id).
		UpdateColumn("last_login_at", lastLoginAt.UTC())
	if result.Error != nil {
		return fmt.Errorf("update user last login time: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}

func (r *userRepository) UpdatePasswordHash(ctx context.Context, id int64, passwordHash string) error {
	result := r.db.WithContext(ctx).
		Model(&model.User{}).
		Where("id = ?", id).
		Update("password_hash", passwordHash)
	if result.Error != nil {
		return fmt.Errorf("update user password hash: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}

func mapUserError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrUserNotFound
	}
	return fmt.Errorf("query user: %w", err)
}

func (r *userRepository) List(ctx context.Context, query UserListQuery) ([]UserRecord, int64, error) {
	if query.Limit <= 0 || query.Offset < 0 {
		return nil, 0, errors.New("invalid user list pagination")
	}
	db := r.db.WithContext(ctx).Model(&model.User{})
	if keyword := strings.TrimSpace(query.Keyword); keyword != "" {
		pattern := containsLikePattern(keyword)
		db = db.Where("(username ILIKE ? ESCAPE '!' OR nickname ILIKE ? ESCAPE '!')", pattern, pattern)
	}
	if query.Status != nil {
		db = db.Where("status = ?", *query.Status)
	}
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count users: %w", err)
	}
	var users []model.User
	if err := db.Omit("password_hash").Order("id DESC").Offset(query.Offset).Limit(query.Limit).Find(&users).Error; err != nil {
		return nil, 0, fmt.Errorf("list users: %w", err)
	}
	ids := make([]int64, 0, len(users))
	for _, user := range users {
		ids = append(ids, user.ID)
	}
	roles, err := loadUserRoleRecords(r.db.WithContext(ctx), ids)
	if err != nil {
		return nil, 0, err
	}
	records := make([]UserRecord, 0, len(users))
	for _, user := range users {
		records = append(records, UserRecord{User: user, Roles: roles[user.ID]})
	}
	return records, total, nil
}

func (r *userRepository) FindWithRoles(ctx context.Context, id int64) (*UserRecord, error) {
	return findUserRecord(r.db.WithContext(ctx), id)
}

func findUserRecord(db *gorm.DB, id int64) (*UserRecord, error) {
	var user model.User
	if err := db.Omit("password_hash").First(&user, id).Error; err != nil {
		return nil, mapUserError(err)
	}
	roles, err := loadUserRoleRecords(db, []int64{id})
	if err != nil {
		return nil, err
	}
	return &UserRecord{User: user, Roles: roles[id]}, nil
}

func loadUserRoleRecords(db *gorm.DB, ids []int64) (map[int64][]model.Role, error) {
	result := make(map[int64][]model.Role, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	var rows []struct {
		model.Role
		UserID int64
	}
	if err := db.Model(&model.Role{}).Select("sys_role.*, sys_user_role.user_id").
		Joins("JOIN sys_user_role ON sys_user_role.role_id = sys_role.id").
		Where("sys_user_role.user_id IN ?", ids).
		Order("sys_role.sort ASC, sys_role.id ASC").Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("load user roles: %w", err)
	}
	for _, row := range rows {
		result[row.UserID] = append(result[row.UserID], row.Role)
	}
	return result, nil
}

func (r *userRepository) CreateWithRoles(ctx context.Context, user *model.User, roleIDs []int64) error {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ids, err := validateUserRoles(tx, roleIDs, nil)
		if err != nil {
			return err
		}
		if err := tx.Create(user).Error; err != nil {
			return err
		}
		return insertUserRoles(tx, user.ID, ids)
	})
	return mapUserWriteError(err)
}

func (r *userRepository) UpdateProfile(ctx context.Context, id int64, update UserProfileUpdate) error {
	// Maps preserve intentional clearing of optional fields and remarks.
	result := r.db.WithContext(ctx).Model(&model.User{}).Where("id = ?", id).Updates(map[string]any{
		"nickname": update.Nickname, "email": update.Email, "phone": update.Phone, "remark": update.Remark,
	})
	if result.Error != nil {
		return mapUserWriteError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}

func (r *userRepository) ReplaceRoles(ctx context.Context, id int64, roleIDs []int64) error {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Serialize complete-set replacement for this user so concurrent saves
		// cannot interleave their delete/insert phases or race user deletion.
		var user model.User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).First(&user).Error; err != nil {
			return mapUserError(err)
		}
		current, err := loadUserRoleRecords(tx, []int64{id})
		if err != nil {
			return err
		}
		if err := protectSuperAdmin(&UserRecord{Roles: current[id]}); err != nil {
			return err
		}
		ids, err := validateUserRoles(tx, roleIDs, current[id])
		if err != nil {
			return err
		}
		oldIDs := make([]int64, 0, len(current[id]))
		for _, role := range current[id] {
			oldIDs = append(oldIDs, role.ID)
		}
		slices.Sort(ids)
		slices.Sort(oldIDs)
		if slices.Equal(ids, oldIDs) {
			return nil
		}
		if err := tx.Where("user_id = ?", id).Delete(&model.UserRole{}).Error; err != nil {
			return err
		}
		return insertUserRoles(tx, id, ids)
	})
	return mapUserWriteError(err)
}

func validateUserRoles(db *gorm.DB, ids []int64, current []model.Role) ([]int64, error) {
	// Availability is checked at read time, without coordinating with role
	// deletion. Concurrent deletion can still leave a stale association.
	ids = slices.Clone(ids)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if len(ids) == 0 {
		return ids, nil
	}
	var roles []model.Role
	if err := db.Where("id IN ?", ids).Find(&roles).Error; err != nil {
		return nil, err
	}
	if len(roles) != len(ids) {
		return nil, ErrUserRoleUnavailable
	}
	for _, role := range roles {
		if role.Code == model.SuperAdminRoleCode {
			return nil, ErrSuperAdminNotAssignable
		}
		if role.Status != model.RecordStatusEnabled && !slices.ContainsFunc(current, func(existing model.Role) bool { return existing.ID == role.ID }) {
			return nil, ErrUserRoleUnavailable
		}
	}
	return ids, nil
}

func (r *userRepository) ValidateRoles(ctx context.Context, ids []int64, current []model.Role) error {
	_, err := validateUserRoles(r.db.WithContext(ctx), ids, current)
	return err
}

func insertUserRoles(db *gorm.DB, userID int64, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	rows := make([]model.UserRole, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, model.UserRole{UserID: userID, RoleID: id})
	}
	return db.Create(&rows).Error
}

func protectSuperAdmin(record *UserRecord) error {
	if slices.ContainsFunc(record.Roles, func(role model.Role) bool {
		return role.Code == model.SuperAdminRoleCode
	}) {
		return ErrSuperAdminProtected
	}
	return nil
}

func (r *userRepository) UpdateStatus(ctx context.Context, id int64, status model.RecordStatus) error {
	db := r.db.WithContext(ctx)
	record, err := findUserRecord(db, id)
	if err != nil {
		return err
	}
	if status == model.RecordStatusDisabled {
		if err := protectSuperAdmin(record); err != nil {
			return err
		}
	}
	result := db.Model(&model.User{}).Where("id = ?", id).Update("status", status)
	if result.Error != nil {
		return mapUserWriteError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrUserNotFound
	}
	return nil
}

func (r *userRepository) Delete(ctx context.Context, id int64) error {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Use the same user-before-associations lock order as role replacement.
		// Otherwise deleting associations first can deadlock with a role save.
		var user model.User
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).First(&user).Error; err != nil {
			return mapUserError(err)
		}
		record, err := findUserRecord(tx, id)
		if err != nil {
			return err
		}
		if err := protectSuperAdmin(record); err != nil {
			return err
		}
		if err := tx.Where("user_id = ?", id).Delete(&model.UserRole{}).Error; err != nil {
			return err
		}
		return tx.Delete(&record.User).Error
	})
	return mapUserWriteError(err)
}

func mapUserWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" {
			switch pgErr.ConstraintName {
			case "uk_sys_user_username_active":
				return ErrUsernameExists
			case "uk_sys_user_email_active":
				return ErrUserEmailExists
			case "uk_sys_user_phone_active":
				return ErrUserPhoneExists
			}
		}
	}
	return fmt.Errorf("persist user: %w", err)
}
