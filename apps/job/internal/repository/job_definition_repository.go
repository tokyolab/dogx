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

func NewJobDefinitionRepository(db *gorm.DB) JobDefinitionRepository {
	return &jobDefinitionRepository{db: db}
}

const listColumns = `j.id, j.name, j.executor_group_id, g.name AS executor_group_name,
 j.processor_name, j.execution_mode, j.cron_expr, j.status,
 CASE WHEN j.status = 1 THEN j.next_trigger_at END AS next_trigger_at, j.created_at, j.updated_at`
const groupJoin = ` LEFT JOIN job_executor_group g ON g.id = j.executor_group_id AND g.deleted_at IS NULL `

func (r *jobDefinitionRepository) List(ctx context.Context, f JobFilter) ([]JobListItem, int64, error) {
	if f.Page < 1 || f.PageSize < 1 || f.PageSize > 200 || f.Page-1 > (1<<63-1)/f.PageSize {
		return nil, 0, errors.New("invalid job pagination")
	}
	where := "j.deleted_at IS NULL"
	args := make([]any, 0, 6)
	if f.ID != nil {
		where += " AND j.id = ?"
		args = append(args, *f.ID)
	}
	if f.Name != "" {
		where += " AND j.name ILIKE ? ESCAPE '!'"
		args = append(args, literalContains(f.Name))
	}
	if f.ExecutorGroupID != nil {
		where += " AND j.executor_group_id = ?"
		args = append(args, *f.ExecutorGroupID)
	}
	if f.Status != nil {
		where += " AND j.status = ?"
		args = append(args, *f.Status)
	}
	var total int64
	if err := r.db.WithContext(ctx).Raw("SELECT COUNT(*) FROM job_definition j WHERE "+where, args...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}
	items := make([]JobListItem, 0)
	args = append(args, f.PageSize, (f.Page-1)*f.PageSize)
	err := r.db.WithContext(ctx).Raw("SELECT "+listColumns+" FROM job_definition j"+groupJoin+"WHERE "+where+" ORDER BY j.id DESC LIMIT ? OFFSET ?", args...).Scan(&items).Error
	return items, total, err
}

func literalContains(value string) string {
	return "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(value) + "%"
}

func (r *jobDefinitionRepository) Get(ctx context.Context, id int64) (*JobDetail, error) {
	var item JobDetail
	result := r.db.WithContext(ctx).Raw("SELECT "+listColumns+`, j.remark, j.post_enabled, j.params,
 j.misfire_policy, j.block_policy, j.max_retries, j.retry_interval_seconds, j.timeout_seconds
 FROM job_definition j`+groupJoin+"WHERE j.id = ? AND j.deleted_at IS NULL", id).Scan(&item)
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, ErrJobNotFound
	}
	return &item, nil
}

func (r *jobDefinitionRepository) Lock(ctx context.Context, id int64) (*model.JobDefinition, error) {
	// The caller holds an explicit transaction. One primary-key row coordinates
	// trigger accounting, enable and delete; instance queries MUST follow separately
	// so READ COMMITTED observes the preceding holder's committed instance.
	var job model.JobDefinition
	result := r.db.WithContext(ctx).Raw(`SELECT id, name, executor_group_id, processor_name,
 execution_mode, post_enabled, params, cron_expr, status, misfire_policy, block_policy,
 max_retries, retry_interval_seconds, timeout_seconds, next_trigger_at, deleted_at
 FROM job_definition WHERE id = ? FOR UPDATE`, id).Scan(&job)
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 || job.DeletedAt.Valid {
		return nil, ErrJobNotFound
	}
	return &job, nil
}

func (r *jobDefinitionRepository) Create(ctx context.Context, j *model.JobDefinition) (int64, error) {
	var id int64
	err := r.db.WithContext(ctx).Raw(`INSERT INTO job_definition
 (name, remark, executor_group_id, processor_name, execution_mode, post_enabled, params,
 cron_expr, status, misfire_policy, block_policy, max_retries, retry_interval_seconds, timeout_seconds, next_trigger_at)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`,
		j.Name, j.Remark, j.ExecutorGroupID, j.ProcessorName, j.ExecutionMode, j.PostEnabled, j.Params,
		j.CronExpr, j.Status, j.MisfirePolicy, j.BlockPolicy, j.MaxRetries, j.RetryIntervalSeconds, j.TimeoutSeconds, j.NextTriggerAt).Scan(&id).Error
	return id, err
}

func (r *jobDefinitionRepository) Update(ctx context.Context, id int64, j *model.JobDefinition) error {
	result := r.db.WithContext(ctx).Exec(`UPDATE job_definition SET name = ?, remark = ?, executor_group_id = ?,
 processor_name = ?, execution_mode = ?, post_enabled = ?, params = ?, cron_expr = ?,
 misfire_policy = ?, block_policy = ?, max_retries = ?, retry_interval_seconds = ?, timeout_seconds = ?, updated_at = CURRENT_TIMESTAMP
 WHERE id = ? AND deleted_at IS NULL AND status = 0`, j.Name, j.Remark, j.ExecutorGroupID, j.ProcessorName,
		j.ExecutionMode, j.PostEnabled, j.Params, j.CronExpr, j.MisfirePolicy, j.BlockPolicy,
		j.MaxRetries, j.RetryIntervalSeconds, j.TimeoutSeconds, id)
	return r.checkWrite(ctx, id, result)
}

func (r *jobDefinitionRepository) Enable(ctx context.Context, id int64, next time.Time) error {
	return r.checkWrite(ctx, id, r.db.WithContext(ctx).Exec("UPDATE job_definition SET status = 1, next_trigger_at = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL AND status = 0", next, id))
}

func (r *jobDefinitionRepository) Disable(ctx context.Context, id int64) error {
	return r.checkWrite(ctx, id, r.db.WithContext(ctx).Exec("UPDATE job_definition SET status = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL", id))
}

func (r *jobDefinitionRepository) Delete(ctx context.Context, id int64) error {
	return r.checkWrite(ctx, id, r.db.WithContext(ctx).
		Where("id = ? AND status = ?", id, basemodel.RecordStatusDisabled).Delete(&model.JobDefinition{}))
}

func (r *jobDefinitionRepository) Advance(ctx context.Context, id int64, next time.Time) error {
	return r.checkWrite(ctx, id, r.db.WithContext(ctx).Exec("UPDATE job_definition SET next_trigger_at = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL AND status = 1", next, id))
}

func (r *jobDefinitionRepository) checkWrite(ctx context.Context, id int64, result *gorm.DB) error {
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 1 {
		return nil
	}
	if result.RowsAffected != 0 {
		return fmt.Errorf("job write affected %d rows", result.RowsAffected)
	}
	var exists bool
	if err := r.db.WithContext(ctx).Raw("SELECT EXISTS (SELECT 1 FROM job_definition WHERE id = ? AND deleted_at IS NULL)", id).Scan(&exists).Error; err != nil {
		return err
	}
	if !exists {
		return ErrJobNotFound
	}
	return ErrJobStateNotAllowed
}

func (r *jobDefinitionRepository) LoadRules(ctx context.Context) ([]Rule, error) {
	rules := make([]Rule, 0)
	err := r.db.WithContext(ctx).Raw("SELECT id, cron_expr FROM job_definition WHERE deleted_at IS NULL AND status = 1").Scan(&rules).Error
	return rules, err
}

func (r *jobDefinitionRepository) ListDue(ctx context.Context, at time.Time, limit int) ([]DueJob, error) {
	if limit < 1 {
		return nil, errors.New("due job batch must contain at least one candidate")
	}
	items := make([]DueJob, 0)
	// Literal status and null predicates allow the generic plan to use the partial index.
	err := r.db.WithContext(ctx).Raw(`SELECT id, next_trigger_at FROM job_definition
 WHERE deleted_at IS NULL AND status = 1 AND next_trigger_at IS NOT NULL AND next_trigger_at <= ?
 ORDER BY next_trigger_at, id LIMIT ?`, at, limit).Scan(&items).Error
	return items, err
}
