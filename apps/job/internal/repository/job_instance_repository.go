package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrInstanceLocked means another transaction holds the requested Instance row.
// The caller must return from its transaction and skip this candidate; it does
// not authorize waiting, child writes, or immediately retrying the same Instance.
var ErrInstanceLocked = errors.New("job instance is locked")

// ErrInstanceNotFound means the requested execution record does not exist.
// Historical records remain readable independently of the current Job or group.
var ErrInstanceNotFound = errors.New("job instance not found")

// InstanceFilter selects persisted rounds independently of current Job state.
// Times bound CreatedAt as [CreatedFrom, CreatedTo); nil omits that bound. JobName
// is a case-insensitive literal substring of the name captured at creation.
// Page is one-based and PageSize is limited to 200.
type InstanceFilter struct {
	Page, PageSize  int64
	ID, JobID       *int64
	JobName         string
	ExecutorGroupID *int64
	Status          *model.InstanceStatus
	TriggerType     *model.TriggerType
	CreatedFrom     *time.Time
	CreatedTo       *time.Time
}

// InstanceListItem excludes the execution snapshot and Task contents. JobName
// is historical; ExecutorGroupName comes from the immutable group identity,
// including its retained soft-deleted row, and is nil if that row is absent.
type InstanceListItem struct {
	ID                int64
	JobID             int64
	JobName           string
	ExecutorGroupID   int64
	ExecutorGroupName *string
	TriggerType       model.TriggerType
	ScheduledAt       *time.Time
	Status            model.InstanceStatus
	StartedAt         *time.Time
	DeadlineAt        *time.Time
	FinishedAt        *time.Time
	EndReason         *string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// ManualIdentity contains the request identity and parameter override needed to
// deduplicate manual triggers. ParamsOverridden distinguishes inherited Job
// parameters from an explicit empty override; OverrideParams is nil without an override.
type ManualIdentity struct {
	ID               int64
	ParamsOverridden bool
	OverrideParams   *string
}

// WaitingInstance projects persisted configuration for a waiting Instance;
// reading it does not grant execution ownership. Execution uses ExecutionConfig
// rather than rereading the Job's current configuration.
type WaitingInstance struct {
	ID              int64
	JobID           int64
	ExecutorGroupID int64
	Status          model.InstanceStatus
	ExecutionConfig model.ExecutionConfig `gorm:"type:jsonb"`
}

// ExecutionInstance is a locked Instance projection for execution authorization.
// ExecutionConfig remains raw so callers can inspect Status before decoding a
// snapshot that an already-started or terminal Instance no longer needs here.
// The projection is only valid for decisions made inside the owning transaction.
type ExecutionInstance struct {
	ID              int64
	JobID           int64
	ExecutorGroupID int64
	Status          model.InstanceStatus
	ExecutionConfig json.RawMessage `gorm:"type:jsonb"`
	StartedAt       *time.Time
	DeadlineAt      *time.Time
	FinishedAt      *time.Time
}

// StoppingInstance is the parent projection locked for a stop decision or one
// cancellation batch. It excludes configuration and result contents. The frozen
// reason chooses the final state; these facts remain valid only in the owning
// transaction, whose commit must be confirmed before publishing any decision.
type StoppingInstance struct {
	ID              int64
	ExecutorGroupID int64
	Status          model.InstanceStatus
	EndReason       *string
	StartedAt       *time.Time
	DeadlineAt      *time.Time
	FinishedAt      *time.Time
}

// RunningCandidate retains only an Instance identity and its original start
// position for bounded recheck pagination. It does not establish timeout,
// incomplete results or permission to create another execution attempt.
type RunningCandidate struct {
	ID        int64
	StartedAt time.Time
}

// InstanceCompletion reads a parent and its singleton ordinary Task from one
// statement snapshot, without locking. A nil TaskID identifies a missing child;
// nullable child fields must not be interpreted as an execution result on their
// own. ExecutionConfig stays raw so callers inspect parent status before decoding
// a snapshot that terminal or stopping parents no longer need for completion.
type InstanceCompletion struct {
	ID              int64
	Status          model.InstanceStatus
	ExecutionConfig json.RawMessage `gorm:"type:jsonb"`
	DeadlineAt      *time.Time
	TaskID          *int64
	TaskStatus      *model.TaskStatus
	ResultSummary   *string
}

// JobInstanceRepository stores trigger snapshots and first-start facts, checks
// active Instances, and reads waiting, running and stopping candidates. Reads do not
// authorize execution; LockForExecution only coordinates the caller's explicit
// transaction. The context governs database requests; canceling it does not
// mark an Instance as canceled.
// A successful transaction-scoped call does not confirm commit; use the result
// from TransactionRunner.
type JobInstanceRepository interface {
	// List returns historical rounds by descending creation time and then ID.
	// Count and page are separate READ COMMITTED statements, so concurrent changes
	// may affect their consistency. Empty pages are non-nil slices. The projection
	// does not load frozen parameters or require an undeleted Job or group.
	List(context.Context, InstanceFilter) ([]InstanceListItem, int64, error)

	// ListRunningCandidates reads at most limit Running parents whose non-NULL
	// started_at is strictly before eligibleBefore, ordered by started_at then ID.
	// eligibleBefore must be nonzero and remain fixed for the whole traversal;
	// callers derive it from a separately read database clock and their age policy.
	// A nil after starts the traversal. Otherwise its ID is positive and StartedAt
	// is nonzero and strictly before the same bound; only later composite positions
	// are returned. limit is positive and bounded by the caller's deployment policy.
	//
	// The query reads no Task, deadline, configuration, audit or current Job data,
	// takes no lock and changes no state. The caller advances after admission only;
	// each page has its own normal statement visibility, so later traversals restart
	// from nil to cover earlier positions committed late. Empty results are non-nil;
	// errors return nil, preserve their cause and do not authorize cursor movement.
	ListRunningCandidates(ctx context.Context, eligibleBefore time.Time, after *RunningCandidate, limit int) ([]RunningCandidate, error)

	// StoppingUpperID returns the largest currently visible STOPPING ID, or zero
	// when none exists. It does not claim work or read child/configuration data.
	StoppingUpperID(context.Context) (int64, error)
	// ListStoppingCandidates returns at most limit ascending STOPPING IDs in
	// (afterID, upperID]. The caller advances its temporary cursor only after
	// admission, and starts later traversals from zero to cover late commits.
	// Both bounds are nonnegative, upperID and limit are positive, and afterID
	// cannot exceed upperID. No transaction or row lock is required.
	ListStoppingCandidates(ctx context.Context, afterID, upperID int64, limit int) ([]int64, error)
	// LockForStop locks one positive parent ID in an explicit caller transaction.
	// nowait is false for a stop decision and true for a cancellation batch. A
	// missing row returns nil, nil; an unavailable NOWAIT lock wraps ErrInstanceLocked
	// and the driver cause. Other failures preserve their causes. The lock excludes
	// concurrent execution authorization and lasts until commit or rollback.
	// No clock, child records, configuration or summary is read by this method.
	LockForStop(ctx context.Context, id int64, nowait bool) (*StoppingInstance, error)

	// RecordStop changes the locked parent from its expected Waiting or Running
	// state to Stopping. reason must be manual_stop or instance_timeout; timeout
	// requires Running and the caller's prior completion/deadline checks. decidedAt
	// is the nonzero database time read after the lock in this same transaction.
	// It freezes reason and summary, clears finished_at, and preserves all start,
	// deadline and configuration facts. The caller must hold LockForStop for id.
	// False, nil means no eligible row matched and must abort a new stop decision;
	// true only describes this transaction's write, never a confirmed commit.
	RecordStop(ctx context.Context, id int64, expected model.InstanceStatus, reason, summary string, decidedAt time.Time) (bool, error)

	// FinishStop conditionally ends the locked Stopping parent only when its
	// frozen reason matches and no unfinished Task remains. manual_stop selects
	// Canceled; instance_timeout selects Failed. It sets database finished/audit
	// times but preserves reason, summary and execution facts. The caller holds
	// the parent lock in this same explicit transaction; no Task can be created or
	// authorized after the stop decision. False, nil means no transition occurred,
	// without a diagnosis read. True is not a commit or proof of executor exit.
	FinishStop(ctx context.Context, id int64, reason string) (bool, error)

	// Exists checks one positive Instance ID without reading or decoding its
	// frozen configuration. It does not require the current Job or group to exist.
	// A missing parent returns false, nil; database failures remain errors. This
	// read takes no lock and does not guarantee existence for a later statement.
	Exists(context.Context, int64) (bool, error)

	// FindByID returns the persisted execution record, including its frozen
	// configuration, regardless of current Job/group existence. A missing row
	// returns nil, ErrInstanceNotFound; other failures retain their causes.
	// id must be positive. This is a management read, not execution authorization.
	FindByID(context.Context, int64) (*model.JobInstance, error)

	// FindCompletion reads one parent and its ordinary child with a LEFT JOIN.
	// A missing parent returns nil, nil; a missing child retains the parent and
	// NULL child fields. It reads no current Job or business result_data, decodes
	// no snapshot and takes no lock. id must be positive. Facts remain subject to
	// concurrent parent transitions, which Complete must recheck atomically.
	FindCompletion(context.Context, int64) (*InstanceCompletion, error)

	// Complete conditionally changes a Running parent to Succeeded or Failed
	// after the caller has confirmed stable, complete Task results. It does not
	// recheck children or the deadline, so already-complete results may finish an
	// expired parent. The summary preserves both NULL and an empty string;
	// the reason is success or execution_failed, independent of child reasons.
	//
	// The method owns one explicit transaction and rejects outer transactions.
	// True, nil confirms one affected row and its commit; false, nil means no
	// eligible parent remained and requires no diagnosis query. Errors return
	// false and retain their causes; a commit error does not prove rollback.
	// FinishedAt is this UPDATE's database clock evaluation, not business exit
	// or exact lock-acquisition/commit time. Other execution facts remain unchanged.
	Complete(ctx context.Context, id int64, target model.InstanceStatus, summary *string) (bool, error)

	// LockForExecution attempts a single primary-key FOR UPDATE NOWAIT lock in
	// the caller's explicit transaction. A missing row returns nil, nil; an
	// unavailable lock returns nil, ErrInstanceLocked with the driver cause.
	// Other errors remain technical failures. It does not decode the snapshot,
	// test execution eligibility, read the clock, or inspect existing Tasks.
	// A successful lock lasts until the transaction commits or rolls back.
	LockForExecution(context.Context, int64) (*ExecutionInstance, error)

	// DatabaseTime reads clock_timestamp in a separate statement on the bound
	// database or transaction. Candidate discovery uses an independent handle
	// once per traversal. Execution and stop decisions read it after acquiring
	// their parent lock; transaction-start or application time cannot replace it.
	DatabaseTime(context.Context) (time.Time, error)

	// StartExecution conditionally changes one Waiting Instance to Running only
	// while started_at, deadline_at and finished_at are all NULL. It writes the
	// supplied start/deadline and uses startedAt for this update's audit time.
	// Callers validate times and hold LockForExecution in the same transaction,
	// then insert the first Task before commit. A false result means no row was
	// changed and must abort first-start accounting; true does not confirm commit.
	StartExecution(ctx context.Context, id int64, startedAt, deadlineAt time.Time) (bool, error)

	// HasActive reports whether the Job has Waiting, Running or Stopping Instances,
	// returning false, nil if none exist. It takes no lock. Before using it for a
	// blocking-policy decision or Job deletion, Lock the Job in the same transaction
	// and issue this query separately to observe the previous holder's commit.
	HasActive(context.Context, int64) (bool, error)

	// HasActiveForGroup reports whether a Waiting, Running or Stopping Instance
	// retains this group's frozen identity, even if its Job was moved or deleted.
	// It does not lock or inspect Task rows: valid unfinished Tasks remain covered
	// by their active parent. For group deletion, hold the group's primary-key lock
	// and first establish that HasGroupReferences is false in a separate statement;
	// this query then observes old-group Instances committed before a Job move.
	HasActiveForGroup(context.Context, int64) (bool, error)

	// FindManual finds a manual trigger by Job ID and request UUID, returning
	// nil, nil if absent. A match does not imply identical request parameters;
	// callers compare override information before accepting a replay or rejecting it.
	FindManual(context.Context, int64, uuid.UUID) (*ManualIdentity, error)

	// Insert stores a trigger snapshot with initial status Waiting, ignoring input
	// ID, audit, and execution-result fields. It returns the new ID, whether it
	// inserted, and an error; a new insertion writes ID and Status back to the input.
	// Manual triggers deduplicate by (job_id, request_id); scheduled and misfire
	// triggers use (job_id, scheduled_at). Only that identity conflict returns
	// 0, false, nil. It neither returns the existing ID, compares parameters, nor
	// confirms business completion. To replay a manual request, issue FindManual
	// separately for a fresh READ COMMITTED statement snapshot. The input must be
	// non-nil and must not be accessed concurrently. Other constraint or database
	// errors are not treated as duplicates. An outer rollback does not undo changes
	// written to the input object.
	Insert(context.Context, *model.JobInstance) (int64, bool, error)

	// MaxWaitingID reads the highest currently visible Waiting Instance ID, or
	// zero if there are no candidates. It takes no lock and reads no execution
	// configuration. The caller fixes this bound for one traversal; later pages
	// have their own statement snapshots and do not acquire execution ownership.
	// Query failures return zero and an error, never an empty-traversal decision.
	MaxWaitingID(context.Context) (int64, error)

	// ListWaitingIDs returns only Waiting Instance IDs in (cursor, upperID], in
	// ascending order. Both positions must be nonnegative, cursor <= upperID, and
	// limit must be from 1 through 128. The caller must also limit reads to its
	// available waiting capacity. An empty interval or result is a non-nil slice.
	// Query failures return nil and an error; they must not advance the cursor.
	//
	// The bound stays fixed for this traversal, and the cursor advances only over
	// handed-off, merged or explicitly skipped candidates. Later traversals start
	// at zero because ID allocation order is not commit order. No current Job or
	// execution-mode filters apply; snapshot validation belongs to authorization.
	ListWaitingIDs(ctx context.Context, cursor, upperID int64, limit int) ([]int64, error)

	// LoadWaiting reads up to 1024 supplied IDs that are still Waiting.
	// Empty input returns a non-nil empty slice. Missing or no-longer-waiting
	// Instances are omitted, and input ordering is not guaranteed.
	LoadWaiting(context.Context, []int64) ([]WaitingInstance, error)
}

type jobInstanceRepository struct{ db *gorm.DB }

// NewJobInstanceRepository binds a non-nil database or transaction handle.
// It does not check connectivity, close the connection, or commit transactions.
// Use TransactionRunner's repository for cross-table accounting.
func NewJobInstanceRepository(db *gorm.DB) (JobInstanceRepository, error) {
	if db == nil {
		return nil, errors.New("job instance repository database is nil")
	}
	return &jobInstanceRepository{db: db}, nil
}

func (r *jobInstanceRepository) LockForStop(ctx context.Context, id int64, nowait bool) (*StoppingInstance, error) {
	if id <= 0 {
		return nil, errors.New("stop instance ID must be positive")
	}
	if !r.inTransaction() {
		return nil, errors.New("stop instance lock requires an explicit transaction")
	}
	lock := clause.Locking{Strength: "UPDATE"}
	if nowait {
		lock.Options = "NOWAIT"
	}
	var instance StoppingInstance
	// The one-row parent lock serializes stop decisions with new execution
	// authorization; cancellation batches also use it before touching children.
	err := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Select("id", "executor_group_id", "status", "end_reason", "started_at", "deadline_at", "finished_at").
		Clauses(lock).Where("id = ?", id).Take(&instance).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		var driverErr *pgconn.PgError
		if nowait && errors.As(err, &driverErr) && driverErr.Code == "55P03" {
			return nil, fmt.Errorf("lock stopping instance: %w: %w", ErrInstanceLocked, err)
		}
		return nil, fmt.Errorf("lock stopping instance: %w", err)
	}
	return &instance, nil
}

func (r *jobInstanceRepository) ListRunningCandidates(ctx context.Context, eligibleBefore time.Time, after *RunningCandidate, limit int) ([]RunningCandidate, error) {
	if eligibleBefore.IsZero() || limit <= 0 || (after != nil &&
		(after.ID <= 0 || after.StartedAt.IsZero() || !after.StartedAt.Before(eligibleBefore))) {
		return nil, errors.New("invalid running instance candidate bounds")
	}
	// Keep the status predicate aligned with the partial start-time index.
	// Timeout and completion decisions belong to a fresh later check, so neither
	// deadline nor child state narrows this candidate read.
	query := r.db.WithContext(ctx).Model(&model.JobInstance{}).Select("id", "started_at").
		Where("status = 2 AND started_at < ?", eligibleBefore)
	if after != nil {
		query = query.Where("(started_at, id) > (?, ?)", after.StartedAt, after.ID)
	}
	items := make([]RunningCandidate, 0)
	if err := query.Order("started_at, id").Limit(limit).Find(&items).Error; err != nil {
		return nil, fmt.Errorf("list running instance candidates: %w", err)
	}
	return items, nil
}

func (r *jobInstanceRepository) StoppingUpperID(ctx context.Context) (int64, error) {
	var ids []int64
	if err := r.db.WithContext(ctx).Model(&model.JobInstance{}).Where("status = 6").
		Order("id DESC").Limit(1).Pluck("id", &ids).Error; err != nil {
		return 0, fmt.Errorf("read stopping instance upper bound: %w", err)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return ids[0], nil
}

func (r *jobInstanceRepository) ListStoppingCandidates(ctx context.Context, afterID, upperID int64, limit int) ([]int64, error) {
	if afterID < 0 || upperID <= 0 || afterID > upperID || limit <= 0 {
		return nil, errors.New("invalid stopping candidate bounds")
	}
	ids := make([]int64, 0)
	if err := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Where("status = 6 AND id > ? AND id <= ?", afterID, upperID).
		Order("id").Limit(limit).Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("list stopping instances: %w", err)
	}
	return ids, nil
}

func (r *jobInstanceRepository) RecordStop(ctx context.Context, id int64, expected model.InstanceStatus, reason, summary string, decidedAt time.Time) (bool, error) {
	if id <= 0 || decidedAt.IsZero() || (expected != model.InstanceWaiting && expected != model.InstanceRunning) ||
		(reason != model.EndReasonManualStop && reason != model.EndReasonInstanceTimeout) ||
		(reason == model.EndReasonInstanceTimeout && expected != model.InstanceRunning) {
		return false, errors.New("invalid instance stop decision")
	}
	if !r.inTransaction() {
		return false, errors.New("instance stop decision requires an explicit transaction")
	}
	result := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Where("id = ? AND status = ? AND status IN (1, 2)", id, expected).
		Updates(map[string]any{"status": model.InstanceStopping, "finished_at": nil,
			"end_reason": reason, "result_summary": summary, "updated_at": decidedAt})
	if result.Error != nil {
		return false, fmt.Errorf("record instance stop decision: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (r *jobInstanceRepository) FinishStop(ctx context.Context, id int64, reason string) (bool, error) {
	if id <= 0 || (reason != model.EndReasonManualStop && reason != model.EndReasonInstanceTimeout) {
		return false, errors.New("invalid instance stop completion")
	}
	if !r.inTransaction() {
		return false, errors.New("instance stop completion requires an explicit transaction")
	}
	target := model.InstanceCanceled
	if reason == model.EndReasonInstanceTimeout {
		target = model.InstanceFailed
	}
	db := r.db.WithContext(ctx)
	unfinished := db.Model(&model.JobTask{}).Select("1").
		Where("instance_id = job_instance.id AND status IN (1, 2, 3)")
	result := db.Model(&model.JobInstance{}).
		Where("job_instance.id = ? AND job_instance.status = 6 AND job_instance.end_reason = ?", id, reason).
		Where("NOT EXISTS (?)", unfinished).
		Updates(map[string]any{"status": target, "finished_at": gorm.Expr("clock_timestamp()"), "updated_at": gorm.Expr("clock_timestamp()")})
	if result.Error != nil {
		return false, fmt.Errorf("finish stopping instance: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (r *jobInstanceRepository) Exists(ctx context.Context, id int64) (bool, error) {
	if id <= 0 {
		return false, errors.New("job instance ID must be positive")
	}
	var exists bool
	db := r.db.WithContext(ctx)
	query := db.Model(&model.JobInstance{}).Select("1").Where("id = ?", id)
	// Only the tableless EXISTS wrapper is SQL; the indexed predicate stays in
	// the model query and never loads a potentially large or unusable snapshot.
	if err := db.Raw("SELECT EXISTS (?)", query).Scan(&exists).Error; err != nil {
		return false, fmt.Errorf("check job instance existence: %w", err)
	}
	return exists, nil
}

func (r *jobInstanceRepository) List(ctx context.Context, f InstanceFilter) ([]InstanceListItem, int64, error) {
	if ctx == nil || f.Page < 1 || f.PageSize < 1 || f.PageSize > 200 || f.Page-1 > int64(^uint(0)>>1)/f.PageSize ||
		(f.ID != nil && *f.ID <= 0) || (f.JobID != nil && *f.JobID <= 0) ||
		(f.ExecutorGroupID != nil && *f.ExecutorGroupID <= 0) ||
		(f.Status != nil && (*f.Status < model.InstanceWaiting || *f.Status > model.InstanceStopping)) ||
		(f.TriggerType != nil && (*f.TriggerType < model.TriggerScheduled || *f.TriggerType > model.TriggerManual)) ||
		(f.CreatedFrom != nil && f.CreatedFrom.IsZero()) || (f.CreatedTo != nil && f.CreatedTo.IsZero()) ||
		(f.CreatedFrom != nil && f.CreatedTo != nil && !f.CreatedFrom.Before(*f.CreatedTo)) {
		return nil, 0, errors.New("invalid instance list query")
	}
	db := r.db.WithContext(ctx).Model(&model.JobInstance{}).Table("job_instance AS i")
	if f.ID != nil {
		db = db.Where("i.id = ?", *f.ID)
	}
	if f.JobID != nil {
		db = db.Where("i.job_id = ?", *f.JobID)
	}
	if f.JobName != "" {
		db = db.Where("i.job_name ILIKE ? ESCAPE '!'", literalContains(f.JobName))
	}
	if f.ExecutorGroupID != nil {
		db = db.Where("i.executor_group_id = ?", *f.ExecutorGroupID)
	}
	if f.Status != nil {
		db = db.Where("i.status = ?", *f.Status)
	}
	if f.TriggerType != nil {
		db = db.Where("i.trigger_type = ?", *f.TriggerType)
	}
	if f.CreatedFrom != nil {
		db = db.Where("i.created_at >= ?", *f.CreatedFrom)
	}
	if f.CreatedTo != nil {
		db = db.Where("i.created_at < ?", *f.CreatedTo)
	}
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count job instances: %w", err)
	}
	items := make([]InstanceListItem, 0)
	// Historical lookup deliberately includes soft-deleted group identities.
	// Only their immutable names are read; credentials never enter the projection.
	err := db.Joins("LEFT JOIN job_executor_group g ON g.id = i.executor_group_id").
		Select("i.id, i.job_id, i.job_name, i.executor_group_id, g.name AS executor_group_name, i.trigger_type, i.scheduled_at, i.status, i.started_at, i.deadline_at, i.finished_at, i.end_reason, i.created_at, i.updated_at").
		Order("i.created_at DESC, i.id DESC").Offset(int((f.Page - 1) * f.PageSize)).Limit(int(f.PageSize)).Find(&items).Error
	if err != nil {
		return nil, 0, fmt.Errorf("list job instances: %w", err)
	}
	return items, total, nil
}

func (r *jobInstanceRepository) FindByID(ctx context.Context, id int64) (*model.JobInstance, error) {
	if id <= 0 {
		return nil, errors.New("job instance ID must be positive")
	}
	var instance model.JobInstance
	err := r.db.WithContext(ctx).Where("id = ?", id).Take(&instance).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrInstanceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find job instance: %w", err)
	}
	return &instance, nil
}

func (r *jobInstanceRepository) FindCompletion(ctx context.Context, id int64) (*InstanceCompletion, error) {
	if id <= 0 {
		return nil, errors.New("completion instance ID must be positive")
	}
	var completion InstanceCompletion
	err := r.db.WithContext(ctx).Table("job_instance AS i").
		Select("i.id, i.status, i.execution_config, i.deadline_at, t.id AS task_id, t.status AS task_status, t.result_summary").
		Joins("LEFT JOIN job_task AS t ON t.instance_id = i.id AND t.task_type = 1").
		Where("i.id = ?", id).Take(&completion).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find job instance completion: %w", err)
	}
	return &completion, nil
}

func (r *jobInstanceRepository) Complete(ctx context.Context, id int64, target model.InstanceStatus, summary *string) (bool, error) {
	if id <= 0 || (target != model.InstanceSucceeded && target != model.InstanceFailed) {
		return false, errors.New("invalid job instance completion")
	}
	if r.inTransaction() {
		return false, errors.New("instance completion requires an independently committing database handle")
	}
	reason := model.EndReasonSuccess
	if target == model.InstanceFailed {
		reason = model.EndReasonExecutionFailed
	}
	var changed bool
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// The conditional UPDATE coordinates with stop and competing completion
		// decisions without a pre-read or an additional explicit parent lock.
		result := tx.Model(&model.JobInstance{}).
			Where("job_instance.id = ? AND job_instance.status = 2", id).
			Updates(map[string]any{
				"status": target, "end_reason": reason, "result_summary": summary,
				"finished_at": gorm.Expr("clock_timestamp()"), "updated_at": gorm.Expr("clock_timestamp()"),
			})
		changed = result.RowsAffected == 1
		return result.Error
	})
	if err != nil {
		return false, fmt.Errorf("complete job instance: %w", err)
	}
	return changed, nil
}

func (r *jobInstanceRepository) inTransaction() bool {
	if r.db.Statement != nil {
		if _, ok := r.db.Statement.ConnPool.(gorm.TxCommitter); ok {
			return true
		}
	}
	if r.db.Config != nil {
		_, ok := r.db.ConnPool.(gorm.TxCommitter)
		return ok
	}
	return false
}

func (r *jobInstanceRepository) LockForExecution(ctx context.Context, id int64) (*ExecutionInstance, error) {
	var instance ExecutionInstance
	// The caller holds an explicit transaction. This one-row primary-key lock
	// prevents duplicate first starts and coordinates with Instance stopping.
	// NOWAIT leaves contested work for a later candidate scan rather than
	// occupying a dispatch worker while another transaction owns the row.
	err := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Select("id", "job_id", "executor_group_id", "status", "execution_config", "started_at", "deadline_at", "finished_at").
		Clauses(clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}).
		Where("id = ?", id).Take(&instance).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, executionInstanceLockError(err)
	}
	return &instance, nil
}

func executionInstanceLockError(err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == "55P03" {
		return fmt.Errorf("lock execution instance: %w: %w", ErrInstanceLocked, err)
	}
	return fmt.Errorf("lock execution instance: %w", err)
}

func (r *jobInstanceRepository) DatabaseTime(ctx context.Context) (time.Time, error) {
	var current time.Time
	// A separate statement lets discovery fix one traversal boundary and lets
	// execution/stop decisions obtain current time after any parent-lock wait.
	if err := r.db.WithContext(ctx).Raw("SELECT clock_timestamp()").Scan(&current).Error; err != nil {
		return time.Time{}, fmt.Errorf("read execution database time: %w", err)
	}
	return current, nil
}

func (r *jobInstanceRepository) StartExecution(ctx context.Context, id int64, startedAt, deadlineAt time.Time) (bool, error) {
	result := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Where("id = ? AND status = 1 AND started_at IS NULL AND deadline_at IS NULL AND finished_at IS NULL", id).
		Updates(map[string]any{
			"status": model.InstanceRunning, "started_at": startedAt,
			"deadline_at": deadlineAt, "updated_at": startedAt,
		})
	if result.Error != nil {
		return false, fmt.Errorf("start job instance execution: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (r *jobInstanceRepository) HasActive(ctx context.Context, id int64) (bool, error) {
	var active bool
	db := r.db.WithContext(ctx)
	// Keep the literal status predicate aligned with the partial active index.
	query := db.Model(&model.JobInstance{}).Select("1").Where("job_id = ? AND status IN (1, 2, 6)", id)
	// GORM requires a FROM for its query builder, so only this tableless EXISTS
	// wrapper is SQL; the filtered subquery still uses the model query chain.
	if err := db.Raw("SELECT EXISTS (?)", query).Scan(&active).Error; err != nil {
		return false, fmt.Errorf("check active job instances: %w", err)
	}
	return active, nil
}

func (r *jobInstanceRepository) HasActiveForGroup(ctx context.Context, groupID int64) (bool, error) {
	var active bool
	db := r.db.WithContext(ctx)
	// Keep the literal state set aligned with the group-reference partial index.
	query := db.Model(&model.JobInstance{}).Select("1").Where("executor_group_id = ? AND status IN (1, 2, 6)", groupID)
	// The tableless EXISTS wrapper avoids loading historical rounds or Task rows.
	if err := db.Raw("SELECT EXISTS (?)", query).Scan(&active).Error; err != nil {
		return false, fmt.Errorf("check executor group active instances: %w", err)
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

func (r *jobInstanceRepository) MaxWaitingID(ctx context.Context) (int64, error) {
	var ids []int64
	err := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Where("status = 1").Order("id DESC").Limit(1).Pluck("id", &ids).Error
	if err != nil {
		return 0, fmt.Errorf("find waiting job instance upper bound: %w", err)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return ids[0], nil
}

func (r *jobInstanceRepository) ListWaitingIDs(ctx context.Context, cursor, upperID int64, limit int) ([]int64, error) {
	if cursor < 0 || upperID < 0 || cursor > upperID || limit < 1 || limit > 128 {
		return nil, errors.New("invalid waiting candidate batch")
	}
	ids := make([]int64, 0)
	if cursor == upperID {
		return ids, nil
	}
	// The cursor is temporary pagination only: a later recovery must start from
	// zero because sequence allocation order is not transaction commit order.
	err := r.db.WithContext(ctx).Model(&model.JobInstance{}).
		Where("status = 1 AND id > ? AND id <= ?", cursor, upperID).Order("id").Limit(limit).Pluck("id", &ids).Error
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
