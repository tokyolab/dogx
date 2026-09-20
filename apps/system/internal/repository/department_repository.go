package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

var (
	ErrDepartmentNotFound      = errors.New("department not found")
	ErrDepartmentNameExists    = errors.New("department name already exists")
	ErrDepartmentHasChildren   = errors.New("department has children")
	ErrDepartmentHasUsers      = errors.New("department has users")
	ErrDepartmentParentInvalid = errors.New("department parent is missing or invalid")
	ErrDepartmentCycle         = errors.New("department parent creates a cycle")
)

type DepartmentRepository interface {
	List(ctx context.Context) ([]model.Department, error)
	FindByID(ctx context.Context, id int64) (*model.Department, error)
	Create(ctx context.Context, department *model.Department) error
	Update(ctx context.Context, id int64, department *model.Department) error
	UpdateStatus(ctx context.Context, id int64, status model.RecordStatus) error
	Delete(ctx context.Context, id int64) error
}

type departmentRepository struct{ db *gorm.DB }

func NewDepartmentRepository(db *gorm.DB) (DepartmentRepository, error) {
	if db == nil {
		return nil, errors.New("department repository database is nil")
	}
	return &departmentRepository{db: db}, nil
}

func (r *departmentRepository) List(ctx context.Context) ([]model.Department, error) {
	if ctx == nil {
		return nil, errors.New("list departments context is nil")
	}
	departments := make([]model.Department, 0)
	if err := r.db.WithContext(ctx).Order("parent_id NULLS FIRST, sort ASC, id ASC").Find(&departments).Error; err != nil {
		return nil, fmt.Errorf("list departments: %w", err)
	}
	return departments, nil
}

func (r *departmentRepository) FindByID(ctx context.Context, id int64) (*model.Department, error) {
	if ctx == nil || id <= 0 {
		return nil, errors.New("invalid find department arguments")
	}
	var department model.Department
	if err := r.db.WithContext(ctx).First(&department, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrDepartmentNotFound
		}
		return nil, fmt.Errorf("find department: %w", err)
	}
	return &department, nil
}

func validateDepartmentTree(departments []model.Department, id int64, candidate *model.Department) error {
	nodes := make(map[int64]model.Department, len(departments))
	for _, department := range departments {
		nodes[department.ID] = department
	}
	if id > 0 {
		if _, ok := nodes[id]; !ok {
			return ErrDepartmentNotFound
		}
	}
	if candidate.ParentID == nil {
		return nil
	}
	parent, exists := nodes[*candidate.ParentID]
	if !exists {
		return ErrDepartmentParentInvalid
	}
	visited := make(map[int64]bool)
	for {
		if parent.ID == id || visited[parent.ID] {
			return ErrDepartmentCycle
		}
		visited[parent.ID] = true
		if parent.ParentID == nil {
			break
		}
		var ok bool
		parent, ok = nodes[*parent.ParentID]
		if !ok {
			return ErrDepartmentParentInvalid
		}
	}
	return nil
}

func (r *departmentRepository) validate(ctx context.Context, id int64, department *model.Department) error {
	departments, err := r.List(ctx)
	if err != nil {
		return err
	}
	return validateDepartmentTree(departments, id, department)
}

func (r *departmentRepository) Create(ctx context.Context, department *model.Department) error {
	if ctx == nil || department == nil {
		return errors.New("invalid create department arguments")
	}
	department.Name = strings.TrimSpace(department.Name)
	if department.Name == "" {
		return errors.New("department name is required")
	}
	if err := r.validate(ctx, 0, department); err != nil {
		return err
	}
	return mapDepartmentWriteError(r.db.WithContext(ctx).Create(department).Error)
}

func (r *departmentRepository) Update(ctx context.Context, id int64, department *model.Department) error {
	if ctx == nil || id <= 0 || department == nil {
		return errors.New("invalid update department arguments")
	}
	department.Name = strings.TrimSpace(department.Name)
	if department.Name == "" {
		return errors.New("department name is required")
	}
	if err := r.validate(ctx, id, department); err != nil {
		return err
	}
	result := r.db.WithContext(ctx).Model(&model.Department{}).Where("id = ?", id).Updates(map[string]any{
		"parent_id": department.ParentID,
		"name":      department.Name,
		"sort":      department.Sort,
		"remark":    department.Remark,
	})
	if err := mapDepartmentWriteError(result.Error); err != nil {
		return err
	}
	if result.RowsAffected == 0 {
		return ErrDepartmentNotFound
	}
	return nil
}

func (r *departmentRepository) UpdateStatus(ctx context.Context, id int64, status model.RecordStatus) error {
	if ctx == nil || id <= 0 {
		return errors.New("invalid update department status arguments")
	}
	result := r.db.WithContext(ctx).Model(&model.Department{}).Where("id = ?", id).Update("status", status)
	if result.Error != nil {
		return fmt.Errorf("update department status: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrDepartmentNotFound
	}
	return nil
}

func (r *departmentRepository) Delete(ctx context.Context, id int64) error {
	if ctx == nil || id <= 0 {
		return errors.New("invalid delete department arguments")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var department model.Department
		if err := tx.First(&department, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrDepartmentNotFound
			}
			return fmt.Errorf("find department for delete: %w", err)
		}
		var child model.Department
		err := tx.Select("id").Where("parent_id = ?", id).Take(&child).Error
		if err == nil {
			return ErrDepartmentHasChildren
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check department children: %w", err)
		}
		var user model.User
		err = tx.Select("id").Where("department_id = ?", id).Take(&user).Error
		if err == nil {
			return ErrDepartmentHasUsers
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check department users: %w", err)
		}
		if err := tx.Delete(&department).Error; err != nil {
			return fmt.Errorf("delete department: %w", err)
		}
		return nil
	})
}

func mapDepartmentWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "uk_sys_department_parent_name_active" {
		return ErrDepartmentNameExists
	}
	return fmt.Errorf("write department: %w", err)
}

var _ DepartmentRepository = (*departmentRepository)(nil)
