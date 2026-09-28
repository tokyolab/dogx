package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/system/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

var (
	ErrDictionaryItemNotFound = errors.New("dictionary item not found")
	ErrDictionaryValueExists  = errors.New("dictionary value exists")
)

type DictionaryItemRepository interface {
	ListByDictionaryIDs(context.Context, []int64) ([]model.DictionaryItem, error)
	FindByID(context.Context, int64) (*model.DictionaryItem, error)
	Create(context.Context, *model.DictionaryItem) error
	Update(context.Context, int64, *model.DictionaryItem) error
	UpdateStatus(context.Context, int64, model.RecordStatus) error
	Delete(context.Context, int64) error
}
type dictionaryItemRepository struct{ db *gorm.DB }

func NewDictionaryItemRepository(db *gorm.DB) (DictionaryItemRepository, error) {
	if db == nil {
		return nil, errors.New("dictionary item repository database is nil")
	}
	return &dictionaryItemRepository{db: db}, nil
}
func (r *dictionaryItemRepository) ListByDictionaryIDs(ctx context.Context, ids []int64) ([]model.DictionaryItem, error) {
	items := make([]model.DictionaryItem, 0)
	if len(ids) == 0 {
		return items, nil
	}
	err := r.db.WithContext(ctx).Where("dictionary_id IN ?", ids).Order("sort, id").Find(&items).Error
	return items, err
}
func (r *dictionaryItemRepository) FindByID(ctx context.Context, id int64) (*model.DictionaryItem, error) {
	var item model.DictionaryItem
	err := r.db.WithContext(ctx).First(&item, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrDictionaryItemNotFound
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}
func (r *dictionaryItemRepository) Create(ctx context.Context, item *model.DictionaryItem) error {
	if item == nil {
		return errors.New("dictionary item is nil")
	}
	return mapDictionaryItemWriteError(r.db.WithContext(ctx).Create(item).Error)
}
func (r *dictionaryItemRepository) Update(ctx context.Context, id int64, item *model.DictionaryItem) error {
	if item == nil {
		return errors.New("dictionary item is nil")
	}
	// Value and dictionary ownership are immutable even if an internal caller supplies them.
	return dictionaryItemWriteResult(r.db.WithContext(ctx).Model(&model.DictionaryItem{}).Where("id = ?", id).Updates(map[string]any{"label": item.Label, "sort": item.Sort, "remark": item.Remark}))
}
func (r *dictionaryItemRepository) UpdateStatus(ctx context.Context, id int64, status model.RecordStatus) error {
	return dictionaryItemWriteResult(r.db.WithContext(ctx).Model(&model.DictionaryItem{}).Where("id = ?", id).Update("status", status))
}
func (r *dictionaryItemRepository) Delete(ctx context.Context, id int64) error {
	return dictionaryItemWriteResult(r.db.WithContext(ctx).Delete(&model.DictionaryItem{}, id))
}
func dictionaryItemWriteResult(result *gorm.DB) error {
	if result.Error != nil {
		return mapDictionaryItemWriteError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrDictionaryItemNotFound
	}
	return nil
}
func mapDictionaryItemWriteError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "uk_sys_dictionary_item_value_active" {
		return ErrDictionaryValueExists
	}
	if err != nil {
		return fmt.Errorf("write dictionary item: %w", err)
	}
	return nil
}
