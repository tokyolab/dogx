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
	ErrDictionaryNotFound   = errors.New("dictionary not found")
	ErrDictionaryCodeExists = errors.New("dictionary code exists")
	ErrDictionaryHasItems   = errors.New("dictionary has items")
)

type DictionaryRepository interface {
	List(context.Context) ([]model.Dictionary, error)
	FindByID(context.Context, int64) (*model.Dictionary, error)
	FindByCodes(context.Context, []string) ([]model.Dictionary, error)
	Create(context.Context, *model.Dictionary) error
	Update(context.Context, int64, *model.Dictionary) error
	UpdateStatus(context.Context, int64, model.RecordStatus) error
	Delete(context.Context, int64) error
}
type dictionaryRepository struct{ db *gorm.DB }

func NewDictionaryRepository(db *gorm.DB) (DictionaryRepository, error) {
	if db == nil {
		return nil, errors.New("dictionary repository database is nil")
	}
	return &dictionaryRepository{db: db}, nil
}
func (r *dictionaryRepository) List(ctx context.Context) ([]model.Dictionary, error) {
	items := make([]model.Dictionary, 0)
	err := r.db.WithContext(ctx).Order("id").Find(&items).Error
	return items, err
}
func (r *dictionaryRepository) FindByCodes(ctx context.Context, codes []string) ([]model.Dictionary, error) {
	items := make([]model.Dictionary, 0)
	if len(codes) == 0 {
		return items, nil
	}
	err := r.db.WithContext(ctx).Where("code IN ?", codes).Order("id").Find(&items).Error
	return items, err
}
func (r *dictionaryRepository) FindByID(ctx context.Context, id int64) (*model.Dictionary, error) {
	var item model.Dictionary
	err := r.db.WithContext(ctx).First(&item, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrDictionaryNotFound
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}
func (r *dictionaryRepository) Create(ctx context.Context, item *model.Dictionary) error {
	if item == nil {
		return errors.New("dictionary is nil")
	}
	return mapDictionaryWriteError(r.db.WithContext(ctx).Create(item).Error)
}
func (r *dictionaryRepository) Update(ctx context.Context, id int64, item *model.Dictionary) error {
	if item == nil {
		return errors.New("dictionary is nil")
	}
	// Code is deliberately excluded: existing business values and cache keys use this stable identifier.
	result := r.db.WithContext(ctx).Model(&model.Dictionary{}).Where("id = ?", id).Updates(map[string]any{"name": item.Name, "remark": item.Remark, "is_public": item.IsPublic})
	return dictionaryWriteResult(result)
}
func (r *dictionaryRepository) UpdateStatus(ctx context.Context, id int64, status model.RecordStatus) error {
	return dictionaryWriteResult(r.db.WithContext(ctx).Model(&model.Dictionary{}).Where("id = ?", id).Update("status", status))
}
func (r *dictionaryRepository) Delete(ctx context.Context, id int64) error {
	result := r.db.WithContext(ctx).Where("id = ?", id).
		Where("NOT EXISTS (SELECT 1 FROM sys_dictionary_item WHERE dictionary_id = ? AND deleted_at IS NULL)", id).
		Delete(&model.Dictionary{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return nil
	}
	if _, err := r.FindByID(ctx, id); err != nil {
		return err
	}
	return ErrDictionaryHasItems
}
func dictionaryWriteResult(result *gorm.DB) error {
	if result.Error != nil {
		return mapDictionaryWriteError(result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrDictionaryNotFound
	}
	return nil
}
func mapDictionaryWriteError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "uk_sys_dictionary_code_active" {
		return ErrDictionaryCodeExists
	}
	if err != nil {
		return fmt.Errorf("write dictionary: %w", err)
	}
	return nil
}
