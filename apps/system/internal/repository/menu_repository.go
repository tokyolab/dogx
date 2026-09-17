package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"gorm.io/gorm"
)

var (
	ErrMenuNotFound        = errors.New("menu not found")
	ErrMenuParentInvalid   = errors.New("menu parent is missing or incompatible")
	ErrMenuCycle           = errors.New("menu parent creates a cycle")
	ErrMenuHasChildren     = errors.New("menu has children")
	ErrMenuRouteNameExists = errors.New("menu route name already exists")
	ErrMenuPathExists      = errors.New("menu path already exists")
)

type MenuRepository interface {
	List(ctx context.Context) ([]model.Menu, error)
	FindByID(ctx context.Context, id int64) (*model.Menu, error)
	Create(ctx context.Context, menu *model.Menu) error
	Update(ctx context.Context, id int64, menu *model.Menu) error
	UpdateStatus(ctx context.Context, id int64, status model.RecordStatus) error
	Delete(ctx context.Context, id int64) error
}

type menuRepository struct{ db *gorm.DB }

func NewMenuRepository(db *gorm.DB) (MenuRepository, error) {
	if db == nil {
		return nil, errors.New("menu repository database is nil")
	}
	return &menuRepository{db: db}, nil
}

// This management API is PC-only. Scope every read and write so mobile nodes
// cannot be edited by supplying their IDs to a PC request.
func (r *menuRepository) scoped(ctx context.Context) *gorm.DB {
	return r.db.WithContext(ctx).Model(&model.Menu{}).Where("app_code = ?", model.MenuAppAdminWeb)
}

func (r *menuRepository) List(ctx context.Context) ([]model.Menu, error) {
	if ctx == nil {
		return nil, errors.New("list menus context is nil")
	}
	menus := make([]model.Menu, 0)
	if err := r.scoped(ctx).Order("sort ASC, id ASC").Find(&menus).Error; err != nil {
		return nil, fmt.Errorf("list menus: %w", err)
	}
	return menus, nil
}

func (r *menuRepository) FindByID(ctx context.Context, id int64) (*model.Menu, error) {
	if ctx == nil || id <= 0 {
		return nil, errors.New("invalid find menu arguments")
	}
	var menu model.Menu
	if err := r.scoped(ctx).Where("id = ?", id).Take(&menu).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrMenuNotFound
		}
		return nil, fmt.Errorf("find menu: %w", err)
	}
	return &menu, nil
}

// Load the tree once, not one ancestor query per level. Both type changes and
// reparenting are checked against existing children; no child is moved implicitly.
func validateMenuTree(menus []model.Menu, id int64, candidate *model.Menu) error {
	nodes := make(map[int64]model.Menu, len(menus))
	for _, menu := range menus {
		nodes[menu.ID] = menu
		if candidate.Type == model.MenuTypeElement && id > 0 && menu.ParentID != nil && *menu.ParentID == id {
			return ErrMenuHasChildren
		}
	}
	if id > 0 {
		if _, ok := nodes[id]; !ok {
			return ErrMenuNotFound
		}
	}
	if candidate.ParentID == nil {
		if candidate.Type == model.MenuTypeElement {
			return ErrMenuParentInvalid
		}
		return nil
	}
	parent, exists := nodes[*candidate.ParentID]
	if !exists || parent.Type == model.MenuTypeElement {
		return ErrMenuParentInvalid
	}
	visited := make(map[int64]bool)
	for {
		if parent.ID == id || visited[parent.ID] {
			return ErrMenuCycle
		}
		visited[parent.ID] = true
		if parent.ParentID == nil {
			break
		}
		parent, exists = nodes[*parent.ParentID]
		if !exists {
			return ErrMenuParentInvalid
		}
	}
	return nil
}

func (r *menuRepository) Create(ctx context.Context, menu *model.Menu) error {
	if ctx == nil || menu == nil {
		return errors.New("invalid create menu arguments")
	}
	menus, err := r.List(ctx)
	if err != nil {
		return err
	}
	if err = validateMenuTree(menus, 0, menu); err != nil {
		return err
	}
	menu.AppCode = model.MenuAppAdminWeb
	return mapMenuWriteError(r.db.WithContext(ctx).Create(menu).Error)
}

func (r *menuRepository) Update(ctx context.Context, id int64, menu *model.Menu) error {
	if ctx == nil || id <= 0 || menu == nil {
		return errors.New("invalid update menu arguments")
	}
	menus, err := r.List(ctx)
	if err != nil {
		return err
	}
	if err = validateMenuTree(menus, id, menu); err != nil {
		return err
	}
	// Map updates persist false, zero and empty fields when the type changes.
	// Status belongs to its dedicated endpoint and is deliberately not updated here.
	result := r.scoped(ctx).Where("id = ?", id).Updates(map[string]any{
		"parent_id": menu.ParentID, "menu_type": menu.Type, "name": menu.Name,
		"route_name": menu.RouteName, "path": menu.Path, "component": menu.Component,
		"permission": menu.Permission, "icon": menu.Icon, "sort": menu.Sort,
		"visible": menu.Visible, "keep_alive": menu.KeepAlive, "external": menu.External, "remark": menu.Remark,
	})
	if err = mapMenuWriteError(result.Error); err != nil {
		return err
	}
	if result.RowsAffected == 0 {
		return ErrMenuNotFound
	}
	return nil
}

func (r *menuRepository) UpdateStatus(ctx context.Context, id int64, status model.RecordStatus) error {
	if ctx == nil || id <= 0 {
		return errors.New("invalid update menu status arguments")
	}
	result := r.scoped(ctx).Where("id = ?", id).Update("status", status)
	if result.Error != nil {
		return fmt.Errorf("update menu status: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrMenuNotFound
	}
	return nil
}

func (r *menuRepository) Delete(ctx context.Context, id int64) error {
	if ctx == nil || id <= 0 {
		return errors.New("invalid delete menu arguments")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		scoped := &menuRepository{db: tx}
		menu, err := scoped.FindByID(ctx, id)
		if err != nil {
			return err
		}
		var child model.Menu
		err = tx.Select("id").Where("parent_id = ?", id).Take(&child).Error
		if err == nil {
			return ErrMenuHasChildren
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check menu children: %w", err)
		}
		// The menu and its grants are one database change; API policies are independent.
		if err = tx.Where("menu_id = ?", id).Delete(&model.RoleMenu{}).Error; err != nil {
			return fmt.Errorf("delete menu grants: %w", err)
		}
		if err = tx.Delete(menu).Error; err != nil {
			return fmt.Errorf("delete menu: %w", err)
		}
		return nil
	})
}

func mapMenuWriteError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		switch pgErr.ConstraintName {
		case "uk_sys_menu_app_route_name_active":
			return ErrMenuRouteNameExists
		case "uk_sys_menu_app_path_active":
			return ErrMenuPathExists
		}
	}
	return fmt.Errorf("write menu: %w", err)
}

var _ MenuRepository = (*menuRepository)(nil)
