package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrJobNotFound        = errors.New("job definition not found")
	ErrJobStateNotAllowed = errors.New("job definition state does not allow this operation")
)

type JobFilter struct {
	Page            int64
	PageSize        int64
	ID              *int64
	Name            string
	ExecutorGroupID *int64
	Status          *basemodel.RecordStatus
}

type JobListItem struct {
	ID                int64
	Name              string
	ExecutorGroupID   int64
	ExecutorGroupName *string
	ProcessorName     string
	ExecutionMode     model.ExecutionMode
	CronExpr          string
	Status            basemodel.RecordStatus
	NextTriggerAt     *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type JobDetail struct {
	model.JobDefinition `gorm:"embedded"`
	ExecutorGroupName   *string
}

type Rule struct {
	ID       int64
	CronExpr string
}
type DueJob struct {
	ID            int64
	NextTriggerAt time.Time
}

type JobDefinitionRepository interface {
	List(context.Context, JobFilter) ([]JobListItem, int64, error)
	Get(context.Context, int64) (*JobDetail, error)
	Lock(context.Context, int64) (*model.JobDefinition, error)
	Create(context.Context, *model.JobDefinition) (int64, error)
	Update(context.Context, int64, *model.JobDefinition) error
	Enable(context.Context, int64, time.Time) error
	Disable(context.Context, int64) error
	Delete(context.Context, int64) error
	Advance(context.Context, int64, time.Time) error
	LoadRules(context.Context) ([]Rule, error)
	ListDue(context.Context, time.Time, int) ([]DueJob, error)
}

type jobDefinitionRepository struct{ db *gorm.DB }

func NewJobDefinitionRepository(db *gorm.DB) (JobDefinitionRepository, error) {
	if db == nil {
		return nil, errors.New("job definition repository database is nil")
	}
	return &jobDefinitionRepository{db: db}, nil
}

const listColumns = `j.id, j.name, j.executor_group_id, g.name AS executor_group_name,
 j.processor_name, j.execution_mode, j.cron_expr, j.status,
 CASE WHEN j.status = 1 THEN j.next_trigger_at END AS next_trigger_at, j.created_at, j.updated_at`
const groupJoin = `LEFT JOIN job_executor_group g ON g.id = j.executor_group_id AND g.deleted_at IS NULL`

func (r *jobDefinitionRepository) List(ctx context.Context, f JobFilter) ([]JobListItem, int64, error) {
	if f.Page < 1 || f.PageSize < 1 || f.PageSize > 200 || f.Page-1 > (1<<63-1)/f.PageSize {
		return nil, 0, errors.New("invalid job pagination")
	}
	db := r.db.WithContext(ctx).Model(&model.JobDefinition{}).Table("job_definition AS j")
	if f.ID != nil {
		db = db.Where("j.id = ?", *f.ID)
	}
	if f.Name != "" {
		db = db.Where("j.name ILIKE ? ESCAPE '!'", literalContains(f.Name))
	}
	if f.ExecutorGroupID != nil {
		db = db.Where("j.executor_group_id = ?", *f.ExecutorGroupID)
	}
	if f.Status != nil {
		db = db.Where("j.status = ?", *f.Status)
	}
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}
	items := make([]JobListItem, 0)
	if err := db.Select(listColumns).Joins(groupJoin).Order("j.id DESC").
		Limit(int(f.PageSize)).Offset(int((f.Page - 1) * f.PageSize)).Find(&items).Error; err != nil {
		return nil, 0, fmt.Errorf("list jobs: %w", err)
	}
	return items, total, nil
}

func literalContains(value string) string {
	return "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(value) + "%"
}

func (r *jobDefinitionRepository) Get(ctx context.Context, id int64) (*JobDetail, error) {
	var item JobDetail
	err := r.db.WithContext(ctx).Model(&model.JobDefinition{}).Table("job_definition AS j").
		Select(listColumns+`, j.remark, j.post_enabled, j.params,
 j.misfire_policy, j.block_policy, j.max_retries, j.retry_interval_seconds, j.timeout_seconds`).
		Joins(groupJoin).Where("j.id = ?", id).Take(&item).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return &item, nil
}

func (r *jobDefinitionRepository) Lock(ctx context.Context, id int64) (*model.JobDefinition, error) {
	// The caller holds an explicit transaction. One primary-key row coordinates
	// trigger accounting, enable and delete; instance queries MUST follow separately
	// so READ COMMITTED observes the preceding holder's committed instance.
	var job model.JobDefinition
	// Unscoped is limited to this coordinating read: lock the physical row first,
	// then reject a deletion committed by the preceding holder of the same lock.
	err := r.db.WithContext(ctx).Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "name", "executor_group_id", "processor_name", "execution_mode", "post_enabled",
			"params", "cron_expr", "status", "misfire_policy", "block_policy", "max_retries",
			"retry_interval_seconds", "timeout_seconds", "next_trigger_at", "deleted_at").
		Where("id = ?", id).Take(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock job: %w", err)
	}
	if job.DeletedAt.Valid {
		return nil, ErrJobNotFound
	}
	return &job, nil
}

func (r *jobDefinitionRepository) Create(ctx context.Context, j *model.JobDefinition) (int64, error) {
	if j == nil {
		return 0, errors.New("job definition is nil")
	}
	// Creation accepts configuration only; a reused model must not carry another
	// row's identity, audit timestamps or deletion marker into the new record.
	record := *j
	record.Base = basemodel.Base{}
	if err := r.db.WithContext(ctx).Create(&record).Error; err != nil {
		return 0, fmt.Errorf("create job: %w", err)
	}
	j.Base = record.Base
	return record.ID, nil
}

func (r *jobDefinitionRepository) Update(ctx context.Context, id int64, j *model.JobDefinition) error {
	if j == nil {
		return errors.New("job definition is nil")
	}
	// Preserve false, zero and empty values while excluding status, progress and
	// audit fields supplied by the caller. The state predicate coordinates enable.
	result := r.db.WithContext(ctx).Model(&model.JobDefinition{}).
		Where("id = ? AND status = ?", id, basemodel.RecordStatusDisabled).
		Updates(map[string]any{
			"name": j.Name, "remark": j.Remark, "executor_group_id": j.ExecutorGroupID,
			"processor_name": j.ProcessorName, "execution_mode": j.ExecutionMode,
			"post_enabled": j.PostEnabled, "params": j.Params, "cron_expr": j.CronExpr,
			"misfire_policy": j.MisfirePolicy, "block_policy": j.BlockPolicy,
			"max_retries": j.MaxRetries, "retry_interval_seconds": j.RetryIntervalSeconds,
			"timeout_seconds": j.TimeoutSeconds,
		})
	return r.checkWrite(ctx, id, result)
}

func (r *jobDefinitionRepository) Enable(ctx context.Context, id int64, next time.Time) error {
	result := r.db.WithContext(ctx).Model(&model.JobDefinition{}).
		Where("id = ? AND status = ?", id, basemodel.RecordStatusDisabled).
		Updates(map[string]any{"status": basemodel.RecordStatusEnabled, "next_trigger_at": next})
	return r.checkWrite(ctx, id, result)
}

func (r *jobDefinitionRepository) Disable(ctx context.Context, id int64) error {
	result := r.db.WithContext(ctx).Model(&model.JobDefinition{}).
		Where("id = ?", id).Update("status", basemodel.RecordStatusDisabled)
	return r.checkWrite(ctx, id, result)
}

func (r *jobDefinitionRepository) Delete(ctx context.Context, id int64) error {
	return r.checkWrite(ctx, id, r.db.WithContext(ctx).
		Where("id = ? AND status = ?", id, basemodel.RecordStatusDisabled).Delete(&model.JobDefinition{}))
}

func (r *jobDefinitionRepository) Advance(ctx context.Context, id int64, next time.Time) error {
	result := r.db.WithContext(ctx).Model(&model.JobDefinition{}).
		Where("id = ? AND status = ?", id, basemodel.RecordStatusEnabled).Update("next_trigger_at", next)
	return r.checkWrite(ctx, id, result)
}

func (r *jobDefinitionRepository) checkWrite(ctx context.Context, id int64, result *gorm.DB) error {
	if result.Error != nil {
		return fmt.Errorf("write job: %w", result.Error)
	}
	if result.RowsAffected == 1 {
		return nil
	}
	if result.RowsAffected != 0 {
		return fmt.Errorf("job write affected %d rows", result.RowsAffected)
	}
	var job model.JobDefinition
	err := r.db.WithContext(ctx).Select("id").Where("id = ?", id).Take(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrJobNotFound
	}
	if err != nil {
		return fmt.Errorf("check job after write: %w", err)
	}
	return ErrJobStateNotAllowed
}

func (r *jobDefinitionRepository) LoadRules(ctx context.Context) ([]Rule, error) {
	rules := make([]Rule, 0)
	err := r.db.WithContext(ctx).Model(&model.JobDefinition{}).
		Select("id", "cron_expr").Where("status = ?", basemodel.RecordStatusEnabled).Find(&rules).Error
	if err != nil {
		return nil, fmt.Errorf("load job rules: %w", err)
	}
	return rules, nil
}

func (r *jobDefinitionRepository) ListDue(ctx context.Context, at time.Time, limit int) ([]DueJob, error) {
	if limit < 1 {
		return nil, errors.New("due job batch must contain at least one candidate")
	}
	items := make([]DueJob, 0)
	// Literal status and null predicates allow the generic plan to use the partial index.
	err := r.db.WithContext(ctx).Model(&model.JobDefinition{}).Select("id", "next_trigger_at").
		Where("status = 1 AND next_trigger_at IS NOT NULL AND next_trigger_at <= ?", at).
		Order("next_trigger_at, id").Limit(limit).Find(&items).Error
	if err != nil {
		return nil, fmt.Errorf("list due jobs: %w", err)
	}
	return items, nil
}
