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
	// ErrJobNotFound means the Job is missing or soft-deleted. Use errors.Is to test it.
	ErrJobNotFound = errors.New("job definition not found")
	// ErrJobStateNotAllowed means a conditional write matched no row, but a follow-up
	// query still found an undeleted Job. Without an explicit transaction row lock,
	// this classification reflects the follow-up query, not the write's snapshot.
	ErrJobStateNotAllowed = errors.New("job definition state does not allow this operation")
)

// JobFilter uses one-based page numbers and a PageSize from 1 through 200.
// Nil pointer fields and an empty Name omit their respective filters. A nonempty
// Name performs a case-insensitive literal substring match; percent signs and
// underscores are not SQL wildcards.
type JobFilter struct {
	Page            int64
	PageSize        int64
	ID              *int64
	Name            string
	ExecutorGroupID *int64
	Status          *basemodel.RecordStatus
}

// JobListItem is a management-list projection that omits large fields such as
// parameters. ExecutorGroupName is nil if the group is missing or deleted.
// NextTriggerAt is nil for disabled Jobs; the stored progress is not cleared.
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

// JobDetail is a management-detail projection. Its group name and NextTriggerAt
// have the same display semantics as JobListItem. Trigger decisions must use
// Lock's record, which exposes the stored progress, rather than this projection.
type JobDetail struct {
	model.JobDefinition `gorm:"embedded"`
	ExecutorGroupName   *string
}

// Rule contains the minimal Cron registration fields, not persisted trigger progress.
type Rule struct {
	ID       int64
	CronExpr string
}

// DueJob is a recovery candidate, not a claim or a guarantee of current eligibility.
type DueJob struct {
	ID            int64
	NextTriggerAt time.Time
}

// JobDefinitionRepository accesses configuration and progress for undeleted Jobs.
// Callers validate business inputs, protect group references, and coordinate
// cross-table operations. Success from a transaction-scoped method confirms only
// that operation; TransactionRunner's result determines whether commit succeeded.
// Database errors preserve their causes. Conditional writes distinguish missing
// Jobs from disallowed states using ErrJobNotFound and ErrJobStateNotAllowed.
type JobDefinitionRepository interface {
	// List returns a page ordered by descending ID and the total matching count.
	// An empty page is a non-nil empty slice. Count and page use separate queries;
	// READ COMMITTED does not guarantee that they share one database snapshot.
	List(context.Context, JobFilter) ([]JobListItem, int64, error)

	// Get returns management details, or nil, ErrJobNotFound if absent.
	// It never returns nil, nil.
	Get(context.Context, int64) (*JobDetail, error)

	// Lock locks the Job by primary key in the caller's explicit transaction.
	// The lock lasts until the transaction ends. The result contains configuration,
	// status, and stored progress needed for triggering and status changes, not
	// all management fields such as Remark. Missing or deleted Jobs return
	// nil, ErrJobNotFound, never nil, nil. Dependent Instance queries must use a
	// separate statement after locking to observe the previous holder's commit.
	Lock(context.Context, int64) (*model.JobDefinition, error)

	// Create inserts a new Job, ignoring the input Base and replacing it with the
	// generated Base. Status and NextTriggerAt are stored as supplied; callers
	// initialize and validate them. The input must be non-nil and must not be
	// accessed concurrently. Rolling back an outer transaction does not undo the
	// Base written to the input object.
	Create(context.Context, *model.JobDefinition) (int64, error)

	// Update changes management configuration only for a disabled Job, preserving
	// supplied zero values and empty strings. Identity, status, progress, and audit
	// fields from the input do not overwrite their columns. The input must be non-nil.
	Update(context.Context, int64, *model.JobDefinition) error

	// Enable enables a disabled Job and stores the supplied next trigger time;
	// it does not parse Cron or calculate that time. An already enabled Job returns
	// ErrJobStateNotAllowed; callers implement idempotent enabling after Lock.
	Enable(context.Context, int64, time.Time) error

	// Disable succeeds even if the Job is already disabled. It preserves trigger
	// progress and does not cancel existing Instances.
	Disable(context.Context, int64) error

	// Delete soft-deletes a disabled Job without removing historical Instances or
	// checking for active ones. Callers must Lock and check active Instances in the
	// same transaction first. Repeated deletion returns ErrJobNotFound.
	Delete(context.Context, int64) error

	// Advance stores the supplied next trigger time for an enabled Job without
	// checking whether it is later than the previous value. Callers must Lock and
	// persist the current trigger decision in the same transaction to avoid
	// overwriting another trigger's progress.
	Advance(context.Context, int64, time.Time) error

	// LoadRules returns IDs and Cron expressions for all enabled Jobs.
	// It does not paginate or guarantee ordering.
	LoadRules(context.Context) ([]Rule, error)

	// ListDue returns enabled Jobs whose NextTriggerAt is at or before the supplied
	// time, ordered by trigger time then ID. The limit must be positive; callers
	// control the scan budget and maximum batch size. It neither locks nor advances
	// progress. Triggering must Lock and recheck state and progress; a candidate
	// alone does not authorize execution.
	ListDue(context.Context, time.Time, int) ([]DueJob, error)
}

type jobDefinitionRepository struct{ db *gorm.DB }

// NewJobDefinitionRepository binds a non-nil database or transaction handle.
// It does not check connectivity, close the connection, or commit transactions.
// Use a repository supplied by TransactionRunner when calling Lock.
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
