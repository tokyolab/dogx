package repository

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/tokyolab/dogx/apps/system/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrRoleMenuUnavailable      = errors.New("menu or its ancestor is unavailable")
	ErrSuperAdminMenusProtected = errors.New("super administrator menus are not configurable")
	ErrRoleMenuRoleDisabled     = errors.New("disabled role menus are not configurable")
)

type RoleMenuRepository interface {
	ListMenuIDs(ctx context.Context, roleIDs []int64) ([]int64, error)
	Replace(ctx context.Context, roleID int64, menuIDs []int64) error
}

type roleMenuRepository struct{ db *gorm.DB }

func NewRoleMenuRepository(db *gorm.DB) (RoleMenuRepository, error) {
	if db == nil {
		return nil, errors.New("role menu repository database is nil")
	}
	return &roleMenuRepository{db: db}, nil
}

func (r *roleMenuRepository) ListMenuIDs(ctx context.Context, roleIDs []int64) ([]int64, error) {
	if ctx == nil {
		return nil, errors.New("list role menus context is nil")
	}
	ids := make([]int64, 0)
	if len(roleIDs) == 0 {
		return ids, nil
	}
	// Do not recheck role status: role membership follows the signed JWT snapshot.
	err := r.db.WithContext(ctx).Model(&model.RoleMenu{}).
		Joins("JOIN sys_menu ON sys_menu.id = sys_role_menu.menu_id").
		Where("sys_role_menu.role_id IN ? AND sys_menu.app_code = ? AND sys_menu.deleted_at IS NULL", roleIDs, model.MenuAppAdminWeb).
		Distinct("sys_role_menu.menu_id").Order("sys_role_menu.menu_id").Pluck("sys_role_menu.menu_id", &ids).Error
	if err != nil {
		return nil, fmt.Errorf("list role menu grants: %w", err)
	}
	return ids, nil
}

func (r *roleMenuRepository) Replace(ctx context.Context, roleID int64, menuIDs []int64) error {
	if ctx == nil || roleID <= 0 {
		return errors.New("invalid replace role menu arguments")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var role model.Role
		// Serialize full-set replacements for this role and coordinate with role
		// deletion; otherwise concurrent DELETE + INSERT can mix two grant sets.
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", roleID).Take(&role).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrRoleNotFound
		}
		if err != nil {
			return fmt.Errorf("lock role menu replacement: %w", err)
		}
		if role.Code == model.SuperAdminRoleCode {
			return ErrSuperAdminMenusProtected
		}
		if role.Status != model.RecordStatusEnabled {
			return ErrRoleMenuRoleDisabled
		}
		menus, err := (&menuRepository{db: tx}).List(ctx)
		if err != nil {
			return err
		}
		ids, err := normalizeRoleMenuIDs(menus, menuIDs)
		if err != nil {
			return err
		}
		current, err := (&roleMenuRepository{db: tx}).ListMenuIDs(ctx, []int64{roleID})
		if err != nil {
			return err
		}
		if slices.Equal(current, ids) {
			return nil
		}
		// Preserve grants for other applications. Include soft-deleted PC nodes
		// in cleanup, but never accept them as new authorizations.
		pcMenus := tx.Unscoped().Model(&model.Menu{}).Select("id").Where("app_code = ?", model.MenuAppAdminWeb)
		if err = tx.Where("role_id = ? AND menu_id IN (?)", roleID, pcMenus).Delete(&model.RoleMenu{}).Error; err != nil {
			return fmt.Errorf("delete role menu grants: %w", err)
		}
		grants := make([]model.RoleMenu, 0, len(ids))
		for _, id := range ids {
			grants = append(grants, model.RoleMenu{RoleID: roleID, MenuID: id})
		}
		if len(grants) > 0 {
			// Bound bind parameters independently of the allowed selection size.
			if err = tx.CreateInBatches(grants, 1000).Error; err != nil {
				return fmt.Errorf("insert role menu grants: %w", err)
			}
		}
		return nil
	})
}

func normalizeRoleMenuIDs(menus []model.Menu, selected []int64) ([]int64, error) {
	nodes := make(map[int64]model.Menu, len(menus))
	for _, menu := range menus {
		nodes[menu.ID] = menu
	}
	granted := make(map[int64]bool, len(selected))
	for _, id := range selected {
		visited := make(map[int64]bool)
		for {
			menu, exists := nodes[id]
			if !exists || visited[id] {
				return nil, ErrRoleMenuUnavailable
			}
			if granted[id] {
				break
			}
			visited[id] = true
			if menu.ParentID == nil {
				break
			}
			id = *menu.ParentID
		}
		for ancestor := range visited {
			granted[ancestor] = true
		}
	}
	ids := make([]int64, 0, len(granted))
	for id := range granted {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, nil
}

var _ RoleMenuRepository = (*roleMenuRepository)(nil)
