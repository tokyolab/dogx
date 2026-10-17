package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrExecutorGroupUnavailable = errors.New("executor group is unavailable")

type ExecutorGroupRepository interface {
	LockAvailable(context.Context, int64) error
	Available(context.Context, int64) (bool, error)
}

type executorGroupRepository struct{ db *gorm.DB }

func NewExecutorGroupRepository(db *gorm.DB) (ExecutorGroupRepository, error) {
	if db == nil {
		return nil, errors.New("executor group repository database is nil")
	}
	return &executorGroupRepository{db: db}, nil
}

func (r *executorGroupRepository) LockAvailable(ctx context.Context, id int64) error {
	// The caller holds an explicit transaction. This primary-key lock prevents
	// a group deletion from racing a new or changed Job reference (group -> Job).
	var group model.ExecutorGroup
	err := r.db.WithContext(ctx).Unscoped().Select("id", "deleted_at").
		Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).Take(&group).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrExecutorGroupUnavailable
	}
	if err != nil {
		return fmt.Errorf("lock executor group: %w", err)
	}
	if group.DeletedAt.Valid {
		return ErrExecutorGroupUnavailable
	}
	return nil
}

func (r *executorGroupRepository) Available(ctx context.Context, id int64) (bool, error) {
	var available bool
	db := r.db.WithContext(ctx)
	query := db.Model(&model.ExecutorGroup{}).Select("1").Where("id = ?", id)
	// GORM query builders require an outer FROM; keep only the tableless EXISTS
	// wrapper as SQL while the model subquery applies the normal soft-delete scope.
	if err := db.Raw("SELECT EXISTS (?)", query).Scan(&available).Error; err != nil {
		return false, fmt.Errorf("check executor group availability: %w", err)
	}
	return available, nil
}
