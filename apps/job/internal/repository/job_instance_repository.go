package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ManualIdentity struct {
	ID               int64
	ParamsOverridden bool
	OverrideParams   *string
}

type WaitingInstance struct {
	ID              int64
	JobID           int64
	ExecutorGroupID int64
	Status          model.InstanceStatus
	ExecutionConfig model.ExecutionConfig `gorm:"type:jsonb"`
}

type JobInstanceRepository interface {
	HasActive(context.Context, int64) (bool, error)
	FindManual(context.Context, int64, uuid.UUID) (*ManualIdentity, error)
	Insert(context.Context, *model.JobInstance) (int64, bool, error)
	ListWaitingIDs(context.Context, int64, int) ([]int64, error)
	LoadWaiting(context.Context, []int64) ([]WaitingInstance, error)
}

type jobInstanceRepository struct{ db *gorm.DB }

func NewJobInstanceRepository(db *gorm.DB) (JobInstanceRepository, error) {
	if db == nil {
		return nil, errors.New("job instance repository database is nil")
	}
	return &jobInstanceRepository{db: db}, nil
}

func (r *jobInstanceRepository) HasActive(ctx context.Context, id int64) (bool, error) {
	var active bool
	db := r.db.WithContext(ctx)
	// Keep the literal status predicate aligned with the partial active index.
	query := db.Model(&model.JobInstance{}).Select("1").Where("job_id = ? AND status IN (1, 2)", id)
	// GORM requires a FROM for its query builder, so only this tableless EXISTS
	// wrapper is SQL; the filtered subquery still uses the model query chain.
	if err := db.Raw("SELECT EXISTS (?)", query).Scan(&active).Error; err != nil {
		return false, fmt.Errorf("check active job instances: %w", err)
	}
	return active, nil
}

func (r *jobInstanceRepository) FindManual(ctx context.Context, id int64, request uuid.UUID) (*ManualIdentity, error) {
	var identity ManualIdentity
	err := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Select("id, params_overridden, CASE WHEN params_overridden THEN execution_config->>'params' END AS override_params").
		Where("job_id = ? AND request_id = ?", id, request).Take(&identity).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find manual job instance: %w", err)
	}
	return &identity, nil
}

func (r *jobInstanceRepository) Insert(ctx context.Context, instance *model.JobInstance) (int64, bool, error) {
	if instance == nil {
		return 0, false, errors.New("job instance is nil")
	}
	identityColumn := "scheduled_at"
	if instance.TriggerType == model.TriggerManual {
		identityColumn = "request_id"
	}
	// A reused input may contain an earlier generated ID or execution fields.
	// Insert only the trigger snapshot; the database initializes audit times and
	// execution fields remain NULL until the execution module advances this round.
	record := *instance
	record.ID, record.Status = 0, model.InstanceWaiting
	// Only the selected trigger identity is ignored. Other constraint and transport
	// errors remain errors, including an unknown transaction-commit outcome.
	result := r.db.WithContext(ctx).Select("job_id", "job_name", "executor_group_id", "trigger_type",
		"scheduled_at", "request_id", "params_overridden", "triggered_by", "execution_config", "status").
		Clauses(clause.OnConflict{
			Columns:     []clause.Column{{Name: "job_id"}, {Name: identityColumn}},
			TargetWhere: clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: identityColumn + " IS NOT NULL"}}},
			DoNothing:   true,
		}, clause.Returning{Columns: []clause.Column{{Name: "id"}}}).Create(&record)
	if result.Error != nil {
		return 0, false, fmt.Errorf("create job instance: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return 0, false, nil
	}
	instance.ID, instance.Status = record.ID, record.Status
	return record.ID, true, nil
}

func (r *jobInstanceRepository) ListWaitingIDs(ctx context.Context, cursor int64, limit int) ([]int64, error) {
	if cursor < 0 || limit < 1 || limit > 1024 {
		return nil, errors.New("invalid waiting candidate batch")
	}
	ids := make([]int64, 0)
	// The cursor is temporary pagination only: a later recovery must start from
	// zero because sequence allocation order is not transaction commit order.
	err := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Where("status = 1 AND id > ?", cursor).Order("id").Limit(limit).Pluck("id", &ids).Error
	if err != nil {
		return nil, fmt.Errorf("list waiting job instance IDs: %w", err)
	}
	return ids, nil
}

func (r *jobInstanceRepository) LoadWaiting(ctx context.Context, ids []int64) ([]WaitingInstance, error) {
	items := make([]WaitingInstance, 0)
	if len(ids) == 0 {
		return items, nil
	}
	if len(ids) > 1024 {
		return nil, errors.New("waiting configuration batch exceeds 1024")
	}
	err := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Select("id", "job_id", "executor_group_id", "status", "execution_config").
		Where("id IN ? AND status = 1", ids).Find(&items).Error
	if err != nil {
		return nil, fmt.Errorf("load waiting job instances: %w", err)
	}
	return items, nil
}
