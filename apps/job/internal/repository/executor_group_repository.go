package repository

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
)

var ErrExecutorGroupUnavailable = errors.New("executor group is unavailable")

type ExecutorGroupRepository interface {
	LockAvailable(context.Context, int64) error
	Available(context.Context, int64) (bool, error)
}

type executorGroupRepository struct{ db *gorm.DB }

func NewExecutorGroupRepository(db *gorm.DB) ExecutorGroupRepository {
	return &executorGroupRepository{db: db}
}

func (r *executorGroupRepository) LockAvailable(ctx context.Context, id int64) error {
	// The caller holds an explicit transaction. This primary-key lock prevents
	// a group deletion from racing a new or changed Job reference (group -> Job).
	var group model.ExecutorGroup
	result := r.db.WithContext(ctx).Raw("SELECT id, deleted_at FROM job_executor_group WHERE id = ? FOR UPDATE", id).Scan(&group)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 || group.DeletedAt != nil {
		return ErrExecutorGroupUnavailable
	}
	return nil
}

func (r *executorGroupRepository) Available(ctx context.Context, id int64) (bool, error) {
	var available bool
	err := r.db.WithContext(ctx).Raw("SELECT EXISTS (SELECT 1 FROM job_executor_group WHERE id = ? AND deleted_at IS NULL)", id).Scan(&available).Error
	return available, err
}
