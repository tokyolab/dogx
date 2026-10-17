package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrExecutorGroupUnavailable means the group is missing or soft-deleted.
// It does not mean that the group has no online executors.
var ErrExecutorGroupUnavailable = errors.New("executor group is unavailable")

// ExecutorGroupRepository checks whether groups referenced by Jobs still exist.
// It does not perform service discovery or check executor liveness.
type ExecutorGroupRepository interface {
	// LockAvailable locks the group by primary key and succeeds only if the group
	// exists and is not deleted. Call it in an explicit transaction before writing
	// a Job reference; the lock lasts until that transaction ends. Lock the group
	// before the Job to preserve the order used by group-deletion reference checks.
	// Missing or deleted groups return ErrExecutorGroupUnavailable; other database
	// errors preserve their underlying causes.
	LockAvailable(context.Context, int64) error

	// Available reports whether the group exists and is not deleted in the query
	// snapshot, returning false, nil otherwise. It takes no lock and does not
	// guarantee later availability, so it cannot replace LockAvailable when
	// creating or changing a Job reference.
	Available(context.Context, int64) (bool, error)
}

type executorGroupRepository struct{ db *gorm.DB }

// NewExecutorGroupRepository binds a non-nil database or transaction handle.
// It does not check connectivity, close the connection, or commit transactions.
// Use TransactionRunner to obtain a transaction-scoped repository.
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
