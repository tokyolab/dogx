package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
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

func NewJobInstanceRepository(db *gorm.DB) JobInstanceRepository {
	return &jobInstanceRepository{db: db}
}

func (r *jobInstanceRepository) HasActive(ctx context.Context, id int64) (bool, error) {
	var active bool
	err := r.db.WithContext(ctx).Raw("SELECT EXISTS (SELECT 1 FROM job_instance WHERE job_id = ? AND status IN (1, 2))", id).Scan(&active).Error
	return active, err
}

func (r *jobInstanceRepository) FindManual(ctx context.Context, id int64, request uuid.UUID) (*ManualIdentity, error) {
	var identity ManualIdentity
	result := r.db.WithContext(ctx).Raw(`SELECT id, params_overridden,
 CASE WHEN params_overridden THEN execution_config->>'params' END AS override_params
 FROM job_instance WHERE job_id = ? AND request_id = ?::uuid`, id, request.String()).Scan(&identity)
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, nil
	}
	return &identity, nil
}

func (r *jobInstanceRepository) Insert(ctx context.Context, instance *model.JobInstance) (int64, bool, error) {
	config, err := instance.ExecutionConfig.Value()
	if err != nil {
		return 0, false, err
	}
	conflict := "(job_id, scheduled_at) WHERE scheduled_at IS NOT NULL"
	if instance.TriggerType == model.TriggerManual {
		conflict = "(job_id, request_id) WHERE request_id IS NOT NULL"
	}
	var id int64
	// Only the selected trigger identity is ignored. Other constraint and transport
	// errors remain errors, including an unknown transaction-commit outcome.
	result := r.db.WithContext(ctx).Raw(`INSERT INTO job_instance
 (job_id, job_name, executor_group_id, trigger_type, scheduled_at, request_id,
 params_overridden, triggered_by, execution_config, status)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, 1)
 ON CONFLICT `+conflict+` DO NOTHING RETURNING id`, instance.JobID, instance.JobName,
		instance.ExecutorGroupID, instance.TriggerType, instance.ScheduledAt, instance.RequestID,
		instance.ParamsOverridden, instance.TriggeredBy, config).Scan(&id)
	return id, result.RowsAffected == 1 && result.Error == nil, result.Error
}

func (r *jobInstanceRepository) ListWaitingIDs(ctx context.Context, cursor int64, limit int) ([]int64, error) {
	if cursor < 0 || limit < 1 || limit > 1024 {
		return nil, errors.New("invalid waiting candidate batch")
	}
	ids := make([]int64, 0)
	// The cursor is temporary pagination only: a later recovery must start from
	// zero because sequence allocation order is not transaction commit order.
	err := r.db.WithContext(ctx).Raw("SELECT id FROM job_instance WHERE status = 1 AND id > ? ORDER BY id LIMIT ?", cursor, limit).Scan(&ids).Error
	return ids, err
}

func (r *jobInstanceRepository) LoadWaiting(ctx context.Context, ids []int64) ([]WaitingInstance, error) {
	items := make([]WaitingInstance, 0)
	if len(ids) == 0 {
		return items, nil
	}
	if len(ids) > 1024 {
		return nil, errors.New("waiting configuration batch exceeds 1024")
	}
	err := r.db.WithContext(ctx).Raw("SELECT id, job_id, executor_group_id, status, execution_config FROM job_instance WHERE id IN ? AND status = 1", ids).Scan(&items).Error
	return items, err
}
