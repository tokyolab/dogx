package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	// ErrInvalidDispatchWrite reports invalid response metadata or an outer
	// transaction where an independent commit is required. Repeating the same
	// call cannot repair these preconditions. BindExecutor still permits an
	// outer transaction; only BUSY and rejection saves require independent commits.
	ErrInvalidDispatchWrite = errors.New("invalid dispatch response write")

	// ErrDispatchWriteRejected marks a definite PostgreSQL data, constraint, or
	// statement/access rejection. The original driver error remains available
	// through errors.As. Stop finite retries of this unchanged response; fixing
	// content, schema, permissions or constraints is outside the save retry loop.
	// This classification is not used for transport errors, cancellation, or
	// transaction/commit uncertainty and does not itself claim an earlier retry
	// failed to commit.
	ErrDispatchWriteRejected = errors.New("dispatch response write rejected")
)

// ExecutorBinding identifies the original dispatch and its accepted executor.
// ExecutorInstanceID must be an RFC 4122 UUIDv4 from that dispatch's ACK, and
// ExecutorAddress must be the direct host:port obtained from the same RPC's
// Peer. Structural validation cannot verify that provenance, reachability, or
// the absence of a shared proxy; callers must establish those conditions.
type ExecutorBinding struct {
	TaskID             int64
	InstanceID         int64
	AttemptNo          int64
	ExecutorInstanceID uuid.UUID
	ExecutorAddress    string
}

// DispatchBusyWrite identifies an attempt that received an explicit
// BUSY response proving it was not accepted. Delay must be positive and includes
// any caller-selected jitter; the repository adds it to one database time at
// PostgreSQL timestamp precision. All identity fields must be positive.
type DispatchBusyWrite struct {
	TaskID     int64
	InstanceID int64
	AttemptNo  int64
	Delay      time.Duration
}

// DispatchRejectionWrite identifies an attempt explicitly rejected
// before admission. MaxRetries and RetryIntervalSeconds are the original
// dispatch's frozen policy, not the current Job configuration. MaxRetries must
// be nonnegative, RetryIntervalSeconds positive and all identity fields positive.
// Summary describes rejection, not a business execution failure, and must not
// include credentials or requests.
type DispatchRejectionWrite struct {
	TaskID               int64
	InstanceID           int64
	AttemptNo            int64
	MaxRetries           int32
	RetryIntervalSeconds int32
	Summary              string
}

// TaskResultTarget is the persisted context for one reported Task. The joined
// Instance supplies ownership and frozen configuration, never current Job data.
// ExecutionConfig deliberately remains undecoded so callers can discard stale
// results before inspecting a snapshot that is no longer needed. This read does
// not authorize a write; SaveResult rechecks the attempt, status and binding.
type TaskResultTarget struct {
	TaskID             int64
	InstanceID         int64
	JobID              int64
	ExecutorGroupID    int64
	AttemptNo          int64
	TaskType           model.TaskType
	Status             model.TaskStatus
	UsedAttempts       int32
	ExecutorInstanceID *uuid.UUID
	ExecutionConfig    json.RawMessage `gorm:"type:jsonb"`
}

// TaskResultWrite contains the original report identity and a previously
// validated state decision. Status must be succeeded, retry-waiting or failed;
// RetryIntervalSeconds must be positive only for retry-waiting. Failed outcomes
// ignore Data, while a successful nil Data explicitly clears old output. The
// caller must first verify ownership and the frozen policy with FindResultTarget.
type TaskResultWrite struct {
	TaskID               int64
	InstanceID           int64
	AttemptNo            int64
	ExecutorInstanceID   uuid.UUID
	Status               model.TaskStatus
	Data                 model.TaskData
	Summary              string
	RetryIntervalSeconds int32
}

// UnboundCandidate retains the identity and original pagination position of a
// running attempt observed without either executor binding. It does not prove
// that business work was never accepted, failed, or has stopped.
type UnboundCandidate struct {
	TaskID              int64
	InstanceID          int64
	AttemptNo           int64
	AttemptRegisteredAt time.Time
}

// UnboundCursor retains the last processed original position in a traversal
// with fixed scan and observation times. It is temporary: a new traversal must
// restart without it to discover earlier positions committed or changed later.
type UnboundCursor struct {
	AttemptRegisteredAt time.Time
	TaskID              int64
}

// UnboundTarget is a bounded reread of a candidate and its retained parent.
// StartedAt and DeadlineAt belong to the Instance. ExecutionConfig contains only
// the frozen schema version, execution mode, Post flag, retry count and interval;
// business parameters and other snapshot fields are not returned. Required JSON
// values retain their original types, missing members become JSON null, and a
// non-object snapshot becomes JSON null. Callers classify stale or ineligible
// rows before strictly decoding these fields. The projection also excludes Task
// output and summaries, current Job data and budgets read from the Task;
// settlement must choose its branch from the updated row.
type UnboundTarget struct {
	TaskID              int64
	InstanceID          int64
	AttemptNo           int64
	TaskType            model.TaskType
	Status              model.TaskStatus
	ExecutorInstanceID  *uuid.UUID
	ExecutorAddress     *string
	AttemptRegisteredAt *time.Time
	InstanceStatus      model.InstanceStatus
	StartedAt           *time.Time
	DeadlineAt          *time.Time
	ExecutionConfig     json.RawMessage `gorm:"type:jsonb"`
}

// UnknownSettlement supplies an original candidate identity and the immutable
// execution budget, including the first opportunity. ProcessedAt is the database
// time obtained for the current bounded reread. ObservationCutoff precedes it
// by the observation period; RetryAt follows it by the frozen retry interval.
// Every time must be nonzero, and all identities and MaxAttempts positive.
type UnknownSettlement struct {
	TaskID            int64
	InstanceID        int64
	ExpectedAttemptNo int64
	MaxAttempts       int64
	ProcessedAt       time.Time
	ObservationCutoff time.Time
	RetryAt           time.Time
}

// SettledTask describes a single confirmed-committed transition to retry-waiting
// or final failure. The attempt number is unchanged. This result authorizes a
// parent-check hint, never a resend or a new attempt without fresh registration.
type SettledTask struct {
	TaskID     int64
	InstanceID int64
	AttemptNo  int64
	Status     model.TaskStatus
}

// TaskCancellation identifies one bounded batch selected under its Stopping
// parent's lock in the same explicit transaction. IDs must be nonempty and
// positive; duplicates do not increase the number of changed rows. Reason is
// the parent's frozen manual_stop or instance_timeout. ProcessedAt is the nonzero
// database time obtained for this batch; Summary describes platform cancellation,
// not confirmed business failure or exit. The caller bounds the batch size.
type TaskCancellation struct {
	InstanceID  int64
	TaskIDs     []int64
	Reason      string
	Summary     string
	ProcessedAt time.Time
}

const unknownResultSummary = "execution result is unknown; executor binding and result were not recorded"

// DispatchCandidate is a due Task observed with its original attempt identity and
// pagination position. It grants no execution ownership. Callers must retain
// the original type, state and attempt when registering; a newer row version
// cannot replace a candidate that has already been handed off.
type DispatchCandidate struct {
	TaskID         int64
	InstanceID     int64
	TaskType       model.TaskType
	Status         model.TaskStatus
	AttemptNo      int64
	NextDispatchAt time.Time
}

// DispatchCursor marks the last handed-off position in one traversal with a fixed
// scan time. It is temporary: later traversals restart without a cursor because
// concurrent commits and state changes can move or introduce earlier candidates.
type DispatchCursor struct {
	NextDispatchAt time.Time
	TaskID         int64
}

// AttemptRegistration supplies the original candidate identity, stage, state
// and frozen execution budget, including the first opportunity. Waiting is only
// valid for a shard or Post with attempt zero; retries require a positive attempt.
// All IDs and MaxAttempts must be positive. RegisteredAt is the nonzero database
// time read after locking and authorizing the running parent in this transaction.
type AttemptRegistration struct {
	TaskID            int64
	InstanceID        int64
	TaskType          model.TaskType
	ExpectedStatus    model.TaskStatus
	ExpectedAttemptNo int64
	MaxAttempts       int64
	RegisteredAt      time.Time
}

// RegisteredTask is the new Task version returned by a registration UPDATE.
// InputData is the fixed input for this stage and is returned without another read.
// The record is valid only inside its owning transaction until commit is confirmed;
// receiving this value alone never authorizes sending the newly numbered attempt.
type RegisteredTask struct {
	TaskID       int64
	InstanceID   int64
	TaskType     model.TaskType
	InputData    model.TaskData
	AttemptNo    int64
	UsedAttempts int32
	RegisteredAt time.Time
}

// PreparationTask is the preparation identity and status needed to decide
// whether stage advancement needs its full result. It contains no business data.
type PreparationTask struct {
	ID     int64
	Status model.TaskStatus
}

// PreparationResult is the preparation identity, state and complete saved output.
// SQL NULL is distinct from an empty JSON array; callers validate the shard list.
type PreparationResult struct {
	ID     int64
	Status model.TaskStatus
	Data   model.TaskData `gorm:"column:result_data;type:jsonb"`
}

// StageTask is a preparation or Post Task's minimal execution fact. ResultSummary is
// populated only for a finally failed preparation read by FindStages; nil is
// distinct from an explicitly empty summary. No business input or output is read.
type StageTask struct {
	ID            int64
	TaskType      model.TaskType
	Status        model.TaskStatus
	ResultSummary *string
}

// StageFacts contains the preparation and Post facts from one statement snapshot.
// A nil stage pointer means no such Task exists, not an unfinished or failed Task.
// The caller decides which facts matter using the parent's frozen configuration.
type StageFacts struct {
	Preparation *StageTask
	Post        *StageTask
}

// ShardCounts is a count-only projection of all business shards in one Instance.
// Total includes every shard state; Succeeded, Failed and Canceled count only
// their respective final states. Other states remain visible as a difference
// from their sum. Normal completion still requires Total = Succeeded + Failed;
// cancellation is not business failure. Zero counts alone do not prove that the
// preparation explicitly produced an empty list. JSON tags define fixed Post
// input, which excludes the cancellation diagnostic.
type ShardCounts struct {
	Total     int64 `json:"total"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Canceled  int64 `json:"-"`
}

// TaskSummary is the management-list projection of execution facts. It excludes
// business result_data and preserves NULL timestamps, bindings and summaries.
// ResultSummary may belong to a previous attempt while the Task is running;
// its presence alone does not prove that the current attempt has finished.
type TaskSummary struct {
	ID                  int64
	InstanceID          int64
	TaskType            model.TaskType
	Status              model.TaskStatus
	AttemptNo           int64
	UsedAttempts        int32
	AttemptRegisteredAt *time.Time
	NextDispatchAt      *time.Time
	ExecutorInstanceID  *uuid.UUID
	ExecutorAddress     *string
	ResultSummary       *string
	EndReason           *string
	StartedAt           *time.Time
	FinishedAt          *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// TaskDataSelection limits a management detail read to one optional document.
// Its zero value reads metadata only and does not inspect either JSONB column.
type TaskDataSelection string

const (
	TaskMetadataOnly TaskDataSelection = ""
	TaskInputData    TaskDataSelection = "input"
	TaskResultData   TaskDataSelection = "result"
	// TaskDetailDataLimit bounds the selected JSONB value after text expansion,
	// in bytes. It does not limit stored execution data or change execution RPCs.
	TaskDetailDataLimit = 8 << 20
)

// ErrTaskNotFound identifies an absent Task management record.
var ErrTaskNotFound = errors.New("job task not found")

// ErrTaskDataTooLarge means the requested JSON text exceeds TaskDetailDataLimit.
// No truncated document or successful detail result is returned.
var ErrTaskDataTooLarge = errors.New("job task data exceeds the management detail limit")

// TaskDetail preserves metadata and, when requested, one JSON document as text.
// Data is nil for SQL NULL or a metadata-only read; JSON null is the text "null".
// JSONB normalization may change formatting but never rounds numbers to float64.
type TaskDetail struct {
	TaskSummary `gorm:"embedded"`
	Data        *string
}

// JobTaskRepository persists execution facts without sending RPCs. Its context
// governs database requests, not business cancellation. A successful call using
// a transaction handle does not confirm its outer commit; callers must not
// dispatch work or publish a saved result before that commit succeeds.
type JobTaskRepository interface {
	// FindDetail reads one positive Task ID without a parent, Job or group lookup.
	// Selection must be metadata-only, input or result. The single unlocked query
	// bounds the selected document's expanded text before returning it to Go;
	// the other document is not read. Missing records return ErrTaskNotFound and
	// oversized selected data returns ErrTaskDataTooLarge, both with a nil result.
	// Other errors retain their cause. A nil result with nil error is never valid.
	FindDetail(context.Context, int64, TaskDataSelection) (*TaskDetail, error)

	// ListCancelTargets reads at most limit distinct saved addresses for one
	// positive Instance ID, in the address column's database collation order.
	// An empty after starts the traversal; otherwise addresses are strictly after
	// that previously admitted address. limit must be between 1 and 32. Every Task
	// state is included and NULL or empty addresses are omitted. One unlocked SQL
	// statement seeks each next distinct address; it does not scan Task pages or
	// load results. Empty results are non-nil. Errors preserve their cause and do
	// not authorize cursor advancement. Separate calls use the caller's normal
	// transaction visibility, so late bindings before the cursor can be missed.
	ListCancelTargets(ctx context.Context, instanceID int64, after string, limit int) ([]string, error)

	// ListUnfinishedIDs reads one positive Instance's current unfinished Task IDs
	// in ascending order with a positive, caller-bounded limit. The caller must
	// hold that Stopping parent's lock in this explicit transaction. It reads no
	// Task contents and takes no child locks; all Task types use the same query.
	// There is no cursor: every batch starts at the smallest remaining ID. Empty
	// results are non-nil slices; query failures return nil and retain their cause.
	ListUnfinishedIDs(ctx context.Context, instanceID int64, limit int) ([]int64, error)

	// CancelUnfinished cancels only waiting, running or retry-waiting rows from
	// the supplied parent and bounded IDs, regardless of Task type or attempt.
	// It clears business output and retry time, sets the supplied reason, summary
	// and batch time, and preserves counts, registration/start times and bindings.
	// The caller holds the Stopping parent lock and selected these IDs in this
	// same explicit transaction. No extra read or explicit child lock is added;
	// UPDATE may still wait for concurrent result writes. Zero changed rows is
	// normal. A positive count is not a commit; errors return zero with their cause.
	CancelUnfinished(context.Context, TaskCancellation) (int64, error)

	// ListUnboundCandidates reads running Tasks with two NULL bindings whose
	// registration time is at or before observationCutoff. Parents must be running,
	// have a non-NULL start, and have a deadline after both that start and scanNow.
	// All Task types are retained; configuration, business output and summaries
	// are not read. This one unlocked JOIN grants no settlement or dispatch right.
	//
	// scanNow and observationCutoff must be nonzero, with the cutoff strictly
	// before scanNow; both stay fixed throughout the traversal. A nil cursor starts
	// at the beginning. A supplied cursor must have a positive Task ID and nonzero
	// registration time at or before the cutoff. Results are ordered by original
	// registration time then Task ID, strictly after the cursor. limit must be
	// positive; the caller selects a bounded page size from its deployment policy.
	//
	// Empty results are non-nil slices. Errors return nil and preserve their cause;
	// they do not advance the cursor. Callers must reread candidates with fresh
	// database time before settlement, and keep the original attempt and position.
	ListUnboundCandidates(ctx context.Context, scanNow, observationCutoff time.Time, cursor *UnboundCursor, limit int) ([]UnboundCandidate, error)

	// FindUnboundTargets rereads at most 16 positive Task IDs with their parents
	// in one unlocked JOIN, without inspecting the current Job or Task output.
	// Frozen configuration is projected to the five required JSON fields described
	// by UnboundTarget; it never returns business parameters or the full snapshot.
	// Empty input returns a non-nil empty slice without querying. Duplicate IDs
	// produce one row; missing Tasks or parents are omitted. Rows are ordered by
	// Task ID, not by caller order, and are not filtered by state or Task type.
	// Errors return nil and preserve their cause, not an empty successful batch.
	FindUnboundTargets(context.Context, []int64) ([]UnboundTarget, error)

	// SettleUnknown conditionally closes one original attempt of any stage
	// after the caller has reread a running parent, valid original times and an
	// unexpired deadline using ProcessedAt and its immutable configuration. That
	// prerequisite read takes no parent lock: a later stop may interleave, but
	// this method never changes the parent or grants another execution attempt.
	//
	// The UPDATE rechecks the Task/Instance/attempt identity, valid type, running
	// state, two NULL bindings and registration time <= ObservationCutoff. CASE
	// uses the row's current used-attempt count and MaxAttempts to choose retry
	// or final failure. Counts and original times remain unchanged. Output is
	// cleared and a fixed platform diagnosis replaces any previous summary.
	//
	// This method requires an independently committing handle and rejects outer
	// transactions. Its own explicit transaction remains required when GORM's
	// default transactions are disabled. A record is returned only after confirmed
	// commit; every error returns nil and preserves its cause, without proving
	// rollback. Nil, nil means no row matched, with no diagnostic reread or retry.
	SettleUnknown(context.Context, UnknownSettlement) (*SettledTask, error)

	// ListDispatchCandidates reads due retries and unregistered waiting shard/Post
	// Tasks joined to running parents whose deadlines are strictly after scanNow.
	// Waiting rows must have zero attempt counts and NULL start/registration times.
	// It preserves the original Task types, states and attempts, and reads no
	// current Job, configuration, business data or bindings. scanNow must
	// be nonzero and fixed for the traversal. A nil cursor starts at the beginning;
	// a supplied cursor must have a positive ID and nonzero time <= scanNow.
	// Results are ordered by next_dispatch_at then ID, strictly after the cursor.
	// limit is from 1 through 128 and must also fit the caller's waiting capacity.
	//
	// Empty results are non-nil slices. Errors return nil and do not advance the
	// cursor. This single query takes no locks and grants no dispatch permission;
	// the parent and original attempt must be rechecked during registration.
	ListDispatchCandidates(ctx context.Context, scanNow time.Time, cursor *DispatchCursor, limit int) ([]DispatchCandidate, error)

	// ListInstanceDispatchCandidates applies the same eligibility, ordering, bounds
	// and unlocked-read contract as ListDispatchCandidates to one positive parent
	// ID. It starts at the first due position and is a bounded post-commit hint read,
	// not a separate source of eligibility or permission to send.
	ListInstanceDispatchCandidates(ctx context.Context, instanceID int64, scanNow time.Time, limit int) ([]DispatchCandidate, error)

	// RegisterAttempt conditionally registers a first shard/Post attempt or retry in
	// the caller's explicit transaction. The caller must first lock its parent,
	// confirm running state, valid original times and unexpired deadline, and read
	// the database time after that lock. This method does not repeat those reads.
	//
	// One UPDATE checks the original Task/Instance/attempt, type, expected state,
	// due time and remaining used-attempt budget. A first registration additionally
	// requires zero counts and NULL start/registration times. It increments both counts,
	// clears old bindings, business output, terminal fields and retry time, and
	// sets the first-start time only on first registration and preserves the last
	// summary. PostgreSQL rejects numeric overflow instead of wrapping; any error
	// requires the caller to roll back.
	// Nil, nil means no row matched, without a diagnostic query or a newer attempt
	// lookup. A returned record describes this transaction's write, not its commit.
	RegisterAttempt(context.Context, AttemptRegistration) (*RegisteredTask, error)

	// ListByInstance returns a page in ascending Task ID order and the total
	// matching count, without reading business result_data. instanceID and page
	// must be positive; pageSize must be from 1 through 200. An empty page is a
	// non-nil slice. Count and page are separate READ COMMITTED statements and
	// may observe different committed states. The method does not establish
	// whether the parent exists; management callers check it separately.
	ListByInstance(ctx context.Context, instanceID, page, pageSize int64) ([]TaskSummary, int64, error)

	// FindPreparation reads only the singleton preparation Task's ID and status.
	// instanceID must be positive. Missing preparation returns nil, nil; database
	// failures preserve their causes. This unlocked read grants no generation right
	// and does not load the preparation output.
	FindPreparation(context.Context, int64) (*PreparationTask, error)

	// HasShards reports whether one positive Instance has any shard Task, regardless
	// of state. It is an indexed, unlocked existence query without business data.
	// False, nil means absent; failures return false and preserve their causes.
	HasShards(context.Context, int64) (bool, error)

	// FindPreparationResult reads one positive Instance's complete preparation
	// output with its ID and status. It has FindPreparation's absence/error contract.
	// Callers only load this bounded output when generation or empty-list validation
	// needs it; the repository does not interpret or normalize the list.
	FindPreparationResult(context.Context, int64) (*PreparationResult, error)

	// FindStages reads one positive Instance's preparation and Post identity and
	// status in one unlocked query, plus the summary of a finally failed preparation.
	// It does not read business JSON, Post summaries or per-shard facts. A successful
	// result is non-nil even if both stages are missing; query failures return nil
	// and retain their cause. These facts do not authorize stage creation or completion.
	FindStages(context.Context, int64) (*StageFacts, error)

	// HasUnfinishedShards reports whether a positive Instance has a waiting, running
	// or retry-waiting business shard. The unlocked existence query stops at the
	// first matching row and reads no content. False, nil does not establish normal
	// completion: canceled and unknown states require CountShards validation, and
	// an empty set needs the preparation's explicit empty-list fact. Errors return
	// false and preserve their cause.
	HasUnfinishedShards(context.Context, int64) (bool, error)

	// CountShards reads one positive Instance's business shard counts in one
	// unlocked aggregate without loading JSON, summaries, attempts or child lists.
	// An empty set returns zero counts; any query error returns zero and its cause.
	// Callers read this only when stage advancement needs final counts, after
	// confirming complete generation and no unfinished shards. Stability depends
	// on the fixed shard set, irreversible final states and no active-record cleanup;
	// the query itself neither locks nor proves those prerequisites. Post creation
	// must obtain these facts before taking the parent lock, not aggregate under it.
	CountShards(context.Context, int64) (ShardCounts, error)

	// FindPost reads only a positive Instance's singleton Post ID and status,
	// assigning TaskTypePost without loading its fixed input, output or summary.
	// Missing Post returns nil, nil; query errors return nil and retain their cause.
	// The caller must hold the parent lock when using this read to decide creation.
	FindPost(context.Context, int64) (*StageTask, error)

	// InsertPost creates a waiting singleton Post with zero attempt counts and
	// immutable JSON input containing Total, Succeeded and Failed. Counts must be
	// nonnegative, Total at most 500, Total-Succeeded equal Failed and Canceled zero.
	// The caller obtains stable final counts outside this transaction, then locks the
	// parent, checks its running state, enabled Post configuration and original
	// deadline using createdAt, and verifies that Post is absent. createdAt is
	// nonzero database time shared by due and audit fields. Creation does not consume
	// an execution opportunity or query shard counts. A singleton conflict is an
	// error requiring rollback, never a successful replay; any error returns ID zero.
	// A returned ID is not commit confirmation and must not yet trigger dispatch.
	InsertPost(context.Context, int64, ShardCounts, time.Time) (int64, error)

	// InsertShards inserts every supplied item as one unregistered waiting shard in
	// one SQL statement in the caller's explicit transaction. The caller must hold
	// the parent lock, authorize its running state/deadline using createdAt, and
	// recheck a successful preparation and absence of shards under that lock. The
	// slice must contain 1 through 500 non-nil valid JSON values; duplicates and JSON
	// null are retained. createdAt is nonzero database time shared by all audit and
	// due times. No input is split, merged or deduplicated. Any error requires outer
	// rollback; nil confirms insertion inside the transaction, never its commit.
	InsertShards(context.Context, int64, []model.TaskData, time.Time) error

	// InsertInitial inserts the singleton ordinary or preparation Task running its
	// first attempt. instanceID must be positive and startedAt is the nonzero database
	// time used by StartExecution. The caller must hold the parent's primary-key
	// lock and have successfully called StartExecution in the same transaction.
	// Registration, first start and both audit timestamps receive that same time.
	// A singleton conflict is an error, not a replay or permission to resend; the
	// caller must roll back the parent transition. Any error returns ID zero.
	InsertInitial(context.Context, int64, model.TaskType, time.Time) (int64, error)

	// BindExecutor conditionally saves both binding fields for the original Task,
	// Instance and attempt, only while running with both fields still NULL. All
	// IDs must be positive and the address must fit 255 characters. It does not
	// inspect the parent's state or deadline, so a stopping parent still permits
	// binding a running child. Existing or partial bindings are never overwritten.
	// False, nil means no row matched, including duplicate or late ACKs; callers
	// must not query to guess the cause, resend work, or cancel on that basis.
	// Database errors return false and preserve their cause. A commit error does
	// not prove rollback; any permitted retry must reuse the original metadata.
	// ErrInvalidDispatchWrite and ErrDispatchWriteRejected stop retries of the
	// unchanged response; other errors do not prove a permanent rejection.
	BindExecutor(context.Context, ExecutorBinding) (bool, error)

	// SaveDispatchBusy conditionally closes the original, unbound running
	// attempt, refunds exactly one used opportunity and delays its next dispatch.
	// AttemptNo and registration/start times never move backward. A duplicate or
	// changed row returns false, nil without another refund or a diagnostic read.
	//
	// Like SaveResult, this requires an independently committing handle, rejects
	// outer transactions, and returns true only after one row and a confirmed
	// commit. Errors return false and preserve their cause; a commit error is not
	// proof of rollback. The single UPDATE reads no parent and is permitted after
	// parent expiry or stopping; it does not authorize sending another attempt.
	// ErrInvalidDispatchWrite and ErrDispatchWriteRejected stop retries of the
	// unchanged response, as they do for BindExecutor.
	SaveDispatchBusy(context.Context, DispatchBusyWrite) (bool, error)

	// SaveDispatchRejection closes an explicitly rejected, unbound running
	// attempt. One UPDATE chooses retry-waiting or final failure using the current
	// persisted UsedAttempts and the frozen policy, without refunding an opportunity
	// or changing attempt/start/registration facts. It never inspects the parent.
	// Its independent commit, zero-row and error contracts match SaveDispatchBusy.
	// All saved timestamps share one database evaluation, not a completion or
	// commit time; row-lock waits may occur after that evaluation.
	SaveDispatchRejection(context.Context, DispatchRejectionWrite) (bool, error)

	// FindResultTarget reads one Task and its actual parent in a single INNER
	// JOIN. A missing Task or parent returns nil, nil; query errors remain errors.
	// It does not decode configuration, inspect parent status or deadlines, or
	// lock rows. taskID must be positive. The caller verifies reported ownership
	// and ignores stale attempts before parsing any still-needed configuration.
	FindResultTarget(context.Context, int64) (*TaskResultTarget, error)

	// SaveResult performs one independently committed conditional UPDATE. It
	// requires a nontransactional handle and rejects an outer transaction, since
	// true, nil confirms both one affected row and this write's commit. False, nil
	// means no eligible row remained, not that an earlier write succeeded; callers
	// must not add a diagnosis query. Errors always return false and retain their
	// cause, including commit errors that do not establish rollback.
	//
	// The update checks the original Task, Instance and attempt, running status,
	// and an absent or matching executor identity. It does not require a running
	// or unexpired parent, and never changes bindings, counters or registration
	// times. All written times share one database evaluation in this UPDATE; this
	// is not an exact lock-acquisition, business-completion or commit timestamp.
	SaveResult(context.Context, TaskResultWrite) (bool, error)

	// CheckResultData independently asks PostgreSQL whether supplied non-nil JSON
	// can be represented as JSONB. It is a diagnostic probe after a failed write,
	// not a preflight for every result. False, nil establishes invalid content;
	// SQL errors or a missing/NULL answer do not. Successful validation returns
	// only the boolean, without fetching content diagnostics. Errors retain driver
	// causes, which callers must not log verbatim because they may contain payload.
	// A nil Data means absent optional output and is not probe input.
	CheckResultData(context.Context, model.TaskData) (bool, error)
}

type jobTaskRepository struct{ db *gorm.DB }

// NewJobTaskRepository binds a non-nil database or transaction handle.
// It does not check connectivity, close the connection, or commit transactions.
func NewJobTaskRepository(db *gorm.DB) (JobTaskRepository, error) {
	if db == nil {
		return nil, errors.New("job task repository database is nil")
	}
	return &jobTaskRepository{db: db}, nil
}

func (r *jobTaskRepository) ListCancelTargets(ctx context.Context, instanceID int64, after string, limit int) ([]string, error) {
	if instanceID <= 0 || limit < 1 || limit > 32 || !utf8.ValidString(after) || utf8.RuneCountInString(after) > 255 {
		return nil, errors.New("invalid cancellation target query")
	}
	addresses := make([]string, 0, limit)
	// DISTINCT with LIMIT can still walk every duplicate index entry. Each
	// recursive step instead seeks the next address through the existing partial
	// index. Anchor, recursive value, comparison and ordering retain the column's
	// varchar(255) type and collation; a different collation would lose that path.
	// The bounded recursive structure is the reason this query uses Raw SQL.
	err := r.db.WithContext(ctx).Raw(`WITH RECURSIVE targets(address, position) AS (
		(SELECT executor_address, 1 FROM job_task
		 WHERE instance_id = ? AND executor_address IS NOT NULL AND executor_address > ?
		 ORDER BY executor_address LIMIT 1)
		UNION ALL
		SELECT following.executor_address, targets.position + 1
		FROM targets CROSS JOIN LATERAL (
			SELECT executor_address FROM job_task
			WHERE instance_id = ? AND executor_address IS NOT NULL AND executor_address > targets.address
			ORDER BY executor_address LIMIT 1
		) AS following
		WHERE targets.position < ?
	) SELECT address FROM targets ORDER BY position`, instanceID, after, instanceID, limit).Scan(&addresses).Error
	if err != nil {
		return nil, fmt.Errorf("list cancellation target addresses: %w", err)
	}
	return addresses, nil
}

func (r *jobTaskRepository) ListUnfinishedIDs(ctx context.Context, instanceID int64, limit int) ([]int64, error) {
	if instanceID <= 0 || limit <= 0 {
		return nil, errors.New("invalid unfinished task query")
	}
	if !r.inTransaction() {
		return nil, errors.New("unfinished task query requires an explicit parent transaction")
	}
	ids := make([]int64, 0)
	err := r.db.WithContext(ctx).Model(&model.JobTask{}).
		Where("instance_id = ? AND status IN (1, 2, 3)", instanceID).
		Order("id").Limit(limit).Pluck("id", &ids).Error
	if err != nil {
		return nil, fmt.Errorf("list unfinished tasks: %w", err)
	}
	return ids, nil
}

func (r *jobTaskRepository) CancelUnfinished(ctx context.Context, write TaskCancellation) (int64, error) {
	if write.InstanceID <= 0 || len(write.TaskIDs) == 0 || write.ProcessedAt.IsZero() ||
		(write.Reason != model.EndReasonManualStop && write.Reason != model.EndReasonInstanceTimeout) {
		return 0, errors.New("invalid task cancellation")
	}
	for _, id := range write.TaskIDs {
		if id <= 0 {
			return 0, errors.New("canceled task IDs must be positive")
		}
	}
	if !r.inTransaction() {
		return 0, errors.New("task cancellation requires an explicit parent transaction")
	}
	result := r.db.WithContext(ctx).Model(&model.JobTask{}).
		Where("instance_id = ? AND id IN ? AND status IN (1, 2, 3)", write.InstanceID, write.TaskIDs).
		Updates(map[string]any{"status": model.TaskCanceled, "next_dispatch_at": nil,
			"finished_at": write.ProcessedAt, "end_reason": write.Reason, "result_data": nil,
			"result_summary": write.Summary, "updated_at": write.ProcessedAt})
	if result.Error != nil {
		return 0, fmt.Errorf("cancel unfinished tasks: %w", result.Error)
	}
	return result.RowsAffected, nil
}

func (r *jobTaskRepository) ListUnboundCandidates(ctx context.Context, scanNow, observationCutoff time.Time, cursor *UnboundCursor, limit int) ([]UnboundCandidate, error) {
	if scanNow.IsZero() || observationCutoff.IsZero() || !observationCutoff.Before(scanNow) || limit < 1 ||
		(cursor != nil && (cursor.TaskID <= 0 || cursor.AttemptRegisteredAt.IsZero() || cursor.AttemptRegisteredAt.After(observationCutoff))) {
		return nil, errors.New("invalid unbound candidate query")
	}
	query := r.db.WithContext(ctx).Table("job_task AS t").
		Select("t.id AS task_id, t.instance_id, t.attempt_no, t.attempt_registered_at").
		Joins("JOIN job_instance AS i ON i.id = t.instance_id").
		Where("t.status = 2 AND t.executor_instance_id IS NULL AND t.executor_address IS NULL").
		Where("t.attempt_registered_at <= ?", observationCutoff).
		Where("i.status = 2 AND i.started_at IS NOT NULL AND i.deadline_at > i.started_at AND i.deadline_at > ?", scanNow)
	if cursor != nil {
		query = query.Where("(t.attempt_registered_at, t.id) > (?, ?)", cursor.AttemptRegisteredAt, cursor.TaskID)
	}
	items := make([]UnboundCandidate, 0)
	if err := query.Order("t.attempt_registered_at, t.id").Limit(limit).Find(&items).Error; err != nil {
		return nil, fmt.Errorf("list unbound candidates: %w", err)
	}
	return items, nil
}

func (r *jobTaskRepository) FindUnboundTargets(ctx context.Context, taskIDs []int64) ([]UnboundTarget, error) {
	if len(taskIDs) > 16 {
		return nil, errors.New("unbound target batch exceeds 16 tasks")
	}
	for _, id := range taskIDs {
		if id <= 0 {
			return nil, errors.New("unbound target task IDs must be positive")
		}
	}
	items := make([]UnboundTarget, 0)
	if len(taskIDs) == 0 {
		return items, nil
	}
	err := r.db.WithContext(ctx).Table("job_task AS task").
		Select(`task.id AS task_id, task.instance_id, task.attempt_no, task.task_type, task.status,
			task.executor_instance_id, task.executor_address, task.attempt_registered_at,
			instance.status AS instance_status, instance.started_at, instance.deadline_at,
			CASE WHEN jsonb_typeof(instance.execution_config) = 'object' THEN jsonb_build_object(
				'schema_version', instance.execution_config -> 'schema_version',
				'execution_mode', instance.execution_config -> 'execution_mode',
				'post_enabled', instance.execution_config -> 'post_enabled',
				'max_retries', instance.execution_config -> 'max_retries',
				'retry_interval_seconds', instance.execution_config -> 'retry_interval_seconds'
			) ELSE 'null'::jsonb END AS execution_config`).
		Joins("INNER JOIN job_instance AS instance ON instance.id = task.instance_id").
		Where("task.id IN ?", taskIDs).Order("task.id").Find(&items).Error
	if err != nil {
		return nil, fmt.Errorf("find unbound task targets: %w", err)
	}
	return items, nil
}

func (r *jobTaskRepository) SettleUnknown(ctx context.Context, write UnknownSettlement) (*SettledTask, error) {
	if write.TaskID <= 0 || write.InstanceID <= 0 || write.ExpectedAttemptNo <= 0 || write.MaxAttempts <= 0 ||
		write.ProcessedAt.IsZero() || write.ObservationCutoff.IsZero() || write.RetryAt.IsZero() ||
		!write.ObservationCutoff.Before(write.ProcessedAt) || !write.RetryAt.After(write.ProcessedAt) {
		return nil, errors.New("invalid unknown-result settlement")
	}
	if r.inTransaction() {
		return nil, errors.New("unknown-result settlement requires an independently committing database handle")
	}
	var settled *SettledTask
	// The explicit transaction preserves commit acknowledgement even if the host
	// disables GORM's default write transactions. RETURNING alone is not a commit.
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task model.JobTask
		// CASE evaluates the current locked row rather than an earlier count.
		// Explicit casts preserve a budget above MaxInt32 and timestamp NULL arms.
		result := tx.Model(&task).Clauses(clause.Returning{Columns: []clause.Column{
			{Name: "id"}, {Name: "instance_id"}, {Name: "attempt_no"}, {Name: "status"},
		}}).
			Where("id = ? AND instance_id = ? AND task_type IN (1, 2, 3, 4) AND attempt_no = ? AND status = 2", write.TaskID, write.InstanceID, write.ExpectedAttemptNo).
			Where("executor_instance_id IS NULL AND executor_address IS NULL AND attempt_registered_at <= ?", write.ObservationCutoff).
			Updates(map[string]any{
				"status":           gorm.Expr("CASE WHEN used_attempts < ?::bigint THEN ?::smallint ELSE ?::smallint END", write.MaxAttempts, model.TaskRetryWaiting, model.TaskFailed),
				"next_dispatch_at": gorm.Expr("CASE WHEN used_attempts < ?::bigint THEN ?::timestamptz ELSE NULL END", write.MaxAttempts, write.RetryAt),
				"finished_at":      gorm.Expr("CASE WHEN used_attempts < ?::bigint THEN NULL ELSE ?::timestamptz END", write.MaxAttempts, write.ProcessedAt),
				"end_reason":       gorm.Expr("CASE WHEN used_attempts < ?::bigint THEN NULL ELSE ? END", write.MaxAttempts, model.EndReasonResultUnknown),
				"result_data":      nil, "result_summary": unknownResultSummary, "updated_at": write.ProcessedAt,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		if result.RowsAffected != 1 || task.ID != write.TaskID || task.InstanceID != write.InstanceID || task.AttemptNo != write.ExpectedAttemptNo ||
			(task.Status != model.TaskRetryWaiting && task.Status != model.TaskFailed) {
			return errors.New("unknown-result settlement returned invalid facts")
		}
		settled = &SettledTask{TaskID: task.ID, InstanceID: task.InstanceID, AttemptNo: task.AttemptNo, Status: task.Status}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("settle unknown result: %w", err)
	}
	return settled, nil
}

func (r *jobTaskRepository) ListDispatchCandidates(ctx context.Context, scanNow time.Time, cursor *DispatchCursor, limit int) ([]DispatchCandidate, error) {
	if scanNow.IsZero() || limit < 1 || limit > 128 || (cursor != nil &&
		(cursor.TaskID <= 0 || cursor.NextDispatchAt.IsZero() || cursor.NextDispatchAt.After(scanNow))) {
		return nil, errors.New("invalid dispatch candidate query")
	}
	query := r.dispatchCandidates(ctx, scanNow)
	if cursor != nil {
		query = query.Where("(t.next_dispatch_at, t.id) > (?, ?)", cursor.NextDispatchAt, cursor.TaskID)
	}
	items := make([]DispatchCandidate, 0)
	if err := query.Order("t.next_dispatch_at, t.id").Limit(limit).Find(&items).Error; err != nil {
		return nil, fmt.Errorf("list dispatch candidates: %w", err)
	}
	return items, nil
}

func (r *jobTaskRepository) ListInstanceDispatchCandidates(ctx context.Context, instanceID int64, scanNow time.Time, limit int) ([]DispatchCandidate, error) {
	if instanceID <= 0 || scanNow.IsZero() || limit < 1 || limit > 128 {
		return nil, errors.New("invalid instance dispatch candidate query")
	}
	items := make([]DispatchCandidate, 0)
	if err := r.dispatchCandidates(ctx, scanNow).Where("t.instance_id = ?", instanceID).
		Order("t.next_dispatch_at, t.id").Limit(limit).Find(&items).Error; err != nil {
		return nil, fmt.Errorf("list instance dispatch candidates: %w", err)
	}
	return items, nil
}

func (r *jobTaskRepository) dispatchCandidates(ctx context.Context, scanNow time.Time) *gorm.DB {
	// Keep the literal outer status predicate so PostgreSQL can use the partial
	// dispatch-due index for both first attempts and retries without reading data.
	return r.db.WithContext(ctx).Table("job_task AS t").
		Select("t.id AS task_id, t.instance_id, t.task_type, t.status, t.attempt_no, t.next_dispatch_at").
		Joins("JOIN job_instance AS i ON i.id = t.instance_id").
		Where("t.status IN (1, 3)").
		Where("t.status = 3 OR (t.status = 1 AND t.task_type IN (3, 4) AND t.attempt_no = 0 AND t.used_attempts = 0 AND t.started_at IS NULL AND t.attempt_registered_at IS NULL)").
		Where("t.next_dispatch_at <= ? AND i.status = 2 AND i.deadline_at > ?", scanNow, scanNow)
}

func (r *jobTaskRepository) RegisterAttempt(ctx context.Context, write AttemptRegistration) (*RegisteredTask, error) {
	first := write.ExpectedStatus == model.TaskWaiting
	if write.TaskID <= 0 || write.InstanceID <= 0 || write.MaxAttempts <= 0 || write.RegisteredAt.IsZero() ||
		write.TaskType < model.TaskTypeOrdinary || write.TaskType > model.TaskTypePost ||
		(first && (write.ExpectedAttemptNo != 0 || (write.TaskType != model.TaskTypeShard && write.TaskType != model.TaskTypePost))) ||
		(!first && (write.ExpectedStatus != model.TaskRetryWaiting || write.ExpectedAttemptNo <= 0)) {
		return nil, errors.New("invalid attempt registration")
	}
	if !r.inTransaction() {
		return nil, errors.New("attempt registration requires an explicit transaction")
	}
	var task model.JobTask
	// The original state, type and attempt prevent a stale candidate from consuming
	// another opportunity. RETURNING also carries fixed input for post-commit send.
	// bigint preserves a budget larger than the used_attempts column's int32 range.
	query := r.db.WithContext(ctx).Model(&task).
		Clauses(clause.Returning{Columns: []clause.Column{
			{Name: "id"}, {Name: "instance_id"}, {Name: "task_type"}, {Name: "input_data"},
			{Name: "attempt_no"}, {Name: "used_attempts"}, {Name: "attempt_registered_at"},
		}}).
		Where("id = ? AND instance_id = ? AND task_type = ? AND status = ? AND attempt_no = ?", write.TaskID, write.InstanceID, write.TaskType, write.ExpectedStatus, write.ExpectedAttemptNo).
		Where("next_dispatch_at <= ? AND used_attempts < ?::bigint", write.RegisteredAt, write.MaxAttempts)
	values := map[string]any{
		"status": model.TaskRunning, "attempt_no": gorm.Expr("attempt_no + 1"), "used_attempts": gorm.Expr("used_attempts + 1"),
		"attempt_registered_at": write.RegisteredAt, "next_dispatch_at": nil,
		"executor_instance_id": nil, "executor_address": nil, "result_data": nil,
		"finished_at": nil, "end_reason": nil, "updated_at": write.RegisteredAt,
	}
	if first {
		query = query.Where("used_attempts = 0 AND started_at IS NULL AND attempt_registered_at IS NULL")
		values["started_at"] = write.RegisteredAt
	}
	result := query.Updates(values)
	if result.Error != nil {
		return nil, fmt.Errorf("register task attempt: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, nil
	}
	if result.RowsAffected != 1 || task.ID != write.TaskID || task.InstanceID != write.InstanceID ||
		task.TaskType != write.TaskType || task.AttemptNo != write.ExpectedAttemptNo+1 || task.AttemptRegisteredAt == nil {
		return nil, errors.New("attempt registration returned invalid facts")
	}
	return &RegisteredTask{TaskID: task.ID, InstanceID: task.InstanceID, TaskType: task.TaskType, InputData: task.InputData,
		AttemptNo: task.AttemptNo, UsedAttempts: task.UsedAttempts, RegisteredAt: *task.AttemptRegisteredAt}, nil
}

const taskSummaryColumns = "id, instance_id, task_type, status, attempt_no, used_attempts, attempt_registered_at, next_dispatch_at, executor_instance_id, executor_address, result_summary, end_reason, started_at, finished_at, created_at, updated_at"

func (r *jobTaskRepository) FindDetail(ctx context.Context, id int64, selection TaskDataSelection) (*TaskDetail, error) {
	if ctx == nil || id <= 0 || (selection != TaskMetadataOnly && selection != TaskInputData && selection != TaskResultData) {
		return nil, errors.New("invalid task detail query")
	}
	db := r.db.WithContext(ctx).Model(&model.JobTask{}).Where("id = ?", id)
	var row struct {
		TaskDetail   `gorm:"embedded"`
		DataTooLarge bool
	}
	if selection == TaskMetadataOnly {
		db = db.Select(taskSummaryColumns)
	} else {
		column := "input_data"
		if selection == TaskResultData {
			column = "result_data"
		}
		// JSONB may expand short numeric input into large text, and TOAST size is
		// not the wire size. CASE prevents an oversized value from crossing the DB
		// connection; size and content share the same statement snapshot.
		db = db.Select(taskSummaryColumns+", COALESCE(octet_length("+column+"::text) > ?, FALSE) AS data_too_large, CASE WHEN octet_length("+column+"::text) <= ? THEN "+column+"::text END AS data",
			TaskDetailDataLimit, TaskDetailDataLimit)
	}
	err := db.Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find job task detail: %w", err)
	}
	if row.DataTooLarge {
		return nil, ErrTaskDataTooLarge
	}
	return &row.TaskDetail, nil
}

func (r *jobTaskRepository) ListByInstance(ctx context.Context, instanceID, page, pageSize int64) ([]TaskSummary, int64, error) {
	if instanceID <= 0 || page < 1 || pageSize < 1 || pageSize > 200 || page-1 > int64(^uint(0)>>1)/pageSize {
		return nil, 0, errors.New("invalid task list pagination")
	}
	db := r.db.WithContext(ctx).Model(&model.JobTask{}).Where("instance_id = ?", instanceID)
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count instance tasks: %w", err)
	}
	items := make([]TaskSummary, 0)
	err := db.Select(taskSummaryColumns).
		Order("id ASC").Limit(int(pageSize)).Offset(int((page - 1) * pageSize)).Find(&items).Error
	if err != nil {
		return nil, 0, fmt.Errorf("list instance tasks: %w", err)
	}
	return items, total, nil
}

func (r *jobTaskRepository) FindPreparation(ctx context.Context, instanceID int64) (*PreparationTask, error) {
	if instanceID <= 0 {
		return nil, errors.New("preparation instance ID must be positive")
	}
	var task PreparationTask
	err := r.db.WithContext(ctx).Model(&model.JobTask{}).Select("id, status").
		Where("instance_id = ? AND task_type = ?", instanceID, model.TaskTypePrepare).Take(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find preparation task: %w", err)
	}
	return &task, nil
}

func (r *jobTaskRepository) HasShards(ctx context.Context, instanceID int64) (bool, error) {
	if instanceID <= 0 {
		return false, errors.New("shard instance ID must be positive")
	}
	var task struct{ ID int64 }
	err := r.db.WithContext(ctx).Model(&model.JobTask{}).Select("id").
		Where("instance_id = ? AND task_type = ?", instanceID, model.TaskTypeShard).Take(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check instance shards: %w", err)
	}
	return true, nil
}

func (r *jobTaskRepository) FindPreparationResult(ctx context.Context, instanceID int64) (*PreparationResult, error) {
	if instanceID <= 0 {
		return nil, errors.New("preparation result instance ID must be positive")
	}
	var task PreparationResult
	err := r.db.WithContext(ctx).Model(&model.JobTask{}).Select("id, status, result_data").
		Where("instance_id = ? AND task_type = ?", instanceID, model.TaskTypePrepare).Take(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find preparation result: %w", err)
	}
	return &task, nil
}

func (r *jobTaskRepository) FindStages(ctx context.Context, instanceID int64) (*StageFacts, error) {
	if instanceID <= 0 {
		return nil, errors.New("stage instance ID must be positive")
	}
	var tasks []StageTask
	err := r.db.WithContext(ctx).Model(&model.JobTask{}).
		Select("id, task_type, status, CASE WHEN task_type = 2 AND status = 5 THEN result_summary ELSE NULL END AS result_summary").
		Where("instance_id = ? AND task_type IN (2, 4)", instanceID).Find(&tasks).Error
	if err != nil {
		return nil, fmt.Errorf("find instance stages: %w", err)
	}
	facts := &StageFacts{}
	for i := range tasks {
		task := &tasks[i]
		switch task.TaskType {
		case model.TaskTypePrepare:
			if facts.Preparation != nil {
				return nil, errors.New("instance has multiple preparation tasks")
			}
			facts.Preparation = task
		case model.TaskTypePost:
			if facts.Post != nil {
				return nil, errors.New("instance has multiple Post tasks")
			}
			facts.Post = task
		default:
			return nil, errors.New("stage query returned an unexpected task type")
		}
	}
	return facts, nil
}

func (r *jobTaskRepository) HasUnfinishedShards(ctx context.Context, instanceID int64) (bool, error) {
	if instanceID <= 0 {
		return false, errors.New("unfinished shard instance ID must be positive")
	}
	var task struct{ ID int64 }
	// Keep the literal unfinished predicate aligned with the existing partial
	// index; cancellation and unknown states are deliberately left to the aggregate.
	err := r.db.WithContext(ctx).Model(&model.JobTask{}).Select("id").
		Where("instance_id = ? AND task_type = 3 AND status IN (1, 2, 3)", instanceID).Take(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check unfinished instance shards: %w", err)
	}
	return true, nil
}

func (r *jobTaskRepository) CountShards(ctx context.Context, instanceID int64) (ShardCounts, error) {
	if instanceID <= 0 {
		return ShardCounts{}, errors.New("shard count instance ID must be positive")
	}
	var counts ShardCounts
	result := r.db.WithContext(ctx).Model(&model.JobTask{}).
		Select("COUNT(*) AS total, COUNT(*) FILTER (WHERE status = 4) AS succeeded, COUNT(*) FILTER (WHERE status = 5) AS failed, COUNT(*) FILTER (WHERE status = 6) AS canceled").
		Where("instance_id = ? AND task_type = 3", instanceID).Scan(&counts)
	if result.Error != nil {
		return ShardCounts{}, fmt.Errorf("count instance shards: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return ShardCounts{}, errors.New("shard aggregate returned no definite count")
	}
	return counts, nil
}

func (r *jobTaskRepository) FindPost(ctx context.Context, instanceID int64) (*StageTask, error) {
	if instanceID <= 0 {
		return nil, errors.New("Post instance ID must be positive")
	}
	var task StageTask
	err := r.db.WithContext(ctx).Model(&model.JobTask{}).Select("id, status").
		Where("instance_id = ? AND task_type = 4", instanceID).Take(&task).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find Post task: %w", err)
	}
	task.TaskType = model.TaskTypePost
	return &task, nil
}

func (r *jobTaskRepository) InsertPost(ctx context.Context, instanceID int64, counts ShardCounts, createdAt time.Time) (int64, error) {
	if instanceID <= 0 || createdAt.IsZero() || counts.Total < 0 || counts.Total > 500 ||
		counts.Succeeded < 0 || counts.Failed < 0 || counts.Canceled != 0 || counts.Succeeded > counts.Total || counts.Total-counts.Succeeded != counts.Failed {
		return 0, errors.New("invalid Post creation")
	}
	if !r.inTransaction() {
		return 0, errors.New("Post creation requires an explicit parent transaction")
	}
	data, err := json.Marshal(counts)
	if err != nil {
		return 0, fmt.Errorf("encode Post statistics: %w", err)
	}
	task := model.JobTask{
		InstanceID: instanceID, TaskType: model.TaskTypePost, Status: model.TaskWaiting,
		InputData: model.TaskData(data), NextDispatchAt: &createdAt, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	// Under the parent lock, a singleton conflict contradicts the creation read.
	// Preserve it so the caller rolls back instead of publishing an existing Task.
	if err := r.db.WithContext(ctx).Create(&task).Error; err != nil {
		return 0, fmt.Errorf("create Post task: %w", err)
	}
	return task.ID, nil
}

func (r *jobTaskRepository) InsertShards(ctx context.Context, instanceID int64, items []model.TaskData, createdAt time.Time) error {
	if instanceID <= 0 || createdAt.IsZero() || len(items) == 0 || len(items) > 500 {
		return errors.New("invalid shard generation")
	}
	if !r.inTransaction() {
		return errors.New("shard generation requires an explicit parent transaction")
	}
	tasks := make([]model.JobTask, len(items))
	for i, item := range items {
		if item == nil || !json.Valid(item) {
			return errors.New("shard input must be a present JSON value")
		}
		tasks[i] = model.JobTask{
			InstanceID: instanceID, TaskType: model.TaskTypeShard, Status: model.TaskWaiting,
			InputData: item, NextDispatchAt: &createdAt, CreatedAt: createdAt, UpdatedAt: createdAt,
		}
	}
	// Override any deployment-wide CreateBatchSize: one statement preserves the
	// approved complete-list insertion and never exposes a partially generated set.
	if err := r.db.WithContext(ctx).Session(&gorm.Session{CreateBatchSize: len(tasks)}).Create(&tasks).Error; err != nil {
		return fmt.Errorf("insert instance shards: %w", err)
	}
	return nil
}

func (r *jobTaskRepository) InsertInitial(ctx context.Context, instanceID int64, taskType model.TaskType, startedAt time.Time) (int64, error) {
	if instanceID <= 0 || startedAt.IsZero() || (taskType != model.TaskTypeOrdinary && taskType != model.TaskTypePrepare) {
		return 0, errors.New("invalid initial task start")
	}
	if !r.inTransaction() {
		return 0, errors.New("initial task creation requires an explicit parent transaction")
	}
	task := model.JobTask{
		InstanceID: instanceID, TaskType: taskType, Status: model.TaskRunning,
		AttemptNo: 1, UsedAttempts: 1, AttemptRegisteredAt: &startedAt, StartedAt: &startedAt,
		CreatedAt: startedAt, UpdatedAt: startedAt,
	}
	// Do not ignore the singleton conflict: under the parent lock it indicates
	// inconsistent first-start state, and the parent transition must roll back.
	if err := r.db.WithContext(ctx).Create(&task).Error; err != nil {
		return 0, fmt.Errorf("create initial job task: %w", err)
	}
	return task.ID, nil
}

func (r *jobTaskRepository) BindExecutor(ctx context.Context, binding ExecutorBinding) (bool, error) {
	if binding.TaskID <= 0 || binding.InstanceID <= 0 || binding.AttemptNo <= 0 ||
		binding.ExecutorInstanceID.Version() != 4 || binding.ExecutorInstanceID.Variant() != uuid.RFC4122 ||
		!validExecutorAddress(binding.ExecutorAddress) {
		return false, fmt.Errorf("%w: invalid task executor binding", ErrInvalidDispatchWrite)
	}
	result := r.db.WithContext(ctx).Model(&model.JobTask{}).
		Where("id = ? AND instance_id = ? AND attempt_no = ? AND status = ?", binding.TaskID, binding.InstanceID, binding.AttemptNo, model.TaskRunning).
		Where("executor_instance_id IS NULL AND executor_address IS NULL").
		Updates(map[string]any{
			"executor_instance_id": binding.ExecutorInstanceID,
			"executor_address":     binding.ExecutorAddress,
			"updated_at":           gorm.Expr("clock_timestamp()"),
		})
	if result.Error != nil {
		return false, dispatchWriteError("bind job task executor", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (r *jobTaskRepository) SaveDispatchBusy(ctx context.Context, write DispatchBusyWrite) (bool, error) {
	if write.TaskID <= 0 || write.InstanceID <= 0 || write.AttemptNo <= 0 || write.Delay <= 0 {
		return false, fmt.Errorf("%w: invalid dispatch BUSY write", ErrInvalidDispatchWrite)
	}
	if r.inTransaction() {
		return false, fmt.Errorf("%w: dispatch BUSY save requires an independently committing database handle", ErrInvalidDispatchWrite)
	}
	// A single volatile clock keeps the delay and audit timestamp consistent
	// without reading the parent or registering another execution opportunity.
	result := r.db.WithContext(ctx).Model(&model.JobTask{}).
		Clauses(clause.From{Tables: []clause.Table{{Name: "(SELECT clock_timestamp() AS processed_at)", Alias: "dispatch_clock", Raw: true}}}).
		Where("job_task.id = ? AND job_task.instance_id = ? AND job_task.attempt_no = ?", write.TaskID, write.InstanceID, write.AttemptNo).
		Where("job_task.task_type IN (1, 2, 3, 4) AND job_task.status = ? AND job_task.used_attempts > 0", model.TaskRunning).
		Where("job_task.executor_instance_id IS NULL AND job_task.executor_address IS NULL").
		Updates(map[string]any{
			"status": model.TaskRetryWaiting, "used_attempts": gorm.Expr("job_task.used_attempts - 1"),
			"next_dispatch_at": gorm.Expr("dispatch_clock.processed_at + ? * INTERVAL '1 second'", write.Delay.Seconds()),
			"result_data":      nil, "result_summary": "executor is busy; task was not accepted",
			"finished_at": nil, "end_reason": nil, "updated_at": gorm.Expr("dispatch_clock.processed_at"),
		})
	if result.Error != nil {
		return false, dispatchWriteError("save job task dispatch BUSY", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (r *jobTaskRepository) SaveDispatchRejection(ctx context.Context, write DispatchRejectionWrite) (bool, error) {
	if write.TaskID <= 0 || write.InstanceID <= 0 || write.AttemptNo <= 0 || write.MaxRetries < 0 || write.RetryIntervalSeconds <= 0 {
		return false, fmt.Errorf("%w: invalid dispatch rejection write", ErrInvalidDispatchWrite)
	}
	if r.inTransaction() {
		return false, fmt.Errorf("%w: dispatch rejection save requires an independently committing database handle", ErrInvalidDispatchWrite)
	}
	totalAttempts := int64(write.MaxRetries) + 1
	// CASE reads the locked row version rather than an earlier projection, so
	// concurrent changes to consumed opportunities cannot select a stale outcome.
	// Explicit types keep the total above MaxInt32 representable and prevent
	// PostgreSQL from resolving otherwise untyped CASE status parameters as text.
	result := r.db.WithContext(ctx).Model(&model.JobTask{}).
		Clauses(clause.From{Tables: []clause.Table{{Name: "(SELECT clock_timestamp() AS processed_at)", Alias: "dispatch_clock", Raw: true}}}).
		Where("job_task.id = ? AND job_task.instance_id = ? AND job_task.attempt_no = ?", write.TaskID, write.InstanceID, write.AttemptNo).
		Where("job_task.task_type IN (1, 2, 3, 4) AND job_task.status = ? AND job_task.used_attempts > 0", model.TaskRunning).
		Where("job_task.executor_instance_id IS NULL AND job_task.executor_address IS NULL").
		Updates(map[string]any{
			"status":           gorm.Expr("CASE WHEN job_task.used_attempts < ?::bigint THEN ?::smallint ELSE ?::smallint END", totalAttempts, model.TaskRetryWaiting, model.TaskFailed),
			"next_dispatch_at": gorm.Expr("CASE WHEN job_task.used_attempts < ?::bigint THEN dispatch_clock.processed_at + ? * INTERVAL '1 second' ELSE NULL END", totalAttempts, write.RetryIntervalSeconds),
			"finished_at":      gorm.Expr("CASE WHEN job_task.used_attempts < ?::bigint THEN NULL ELSE dispatch_clock.processed_at END", totalAttempts),
			"end_reason":       gorm.Expr("CASE WHEN job_task.used_attempts < ?::bigint THEN NULL ELSE ? END", totalAttempts, model.EndReasonDispatchRejected),
			"result_data":      nil, "result_summary": write.Summary, "updated_at": gorm.Expr("dispatch_clock.processed_at"),
		})
	if result.Error != nil {
		return false, dispatchWriteError("save job task dispatch rejection", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (r *jobTaskRepository) FindResultTarget(ctx context.Context, taskID int64) (*TaskResultTarget, error) {
	if taskID <= 0 {
		return nil, errors.New("result task ID must be positive")
	}
	var target TaskResultTarget
	err := r.db.WithContext(ctx).Table("job_task AS task").
		Select("task.id AS task_id, task.instance_id, instance.job_id, instance.executor_group_id, task.attempt_no, task.task_type, task.status, task.used_attempts, task.executor_instance_id, instance.execution_config").
		Joins("INNER JOIN job_instance AS instance ON instance.id = task.instance_id").
		Where("task.id = ?", taskID).Take(&target).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find job task result target: %w", err)
	}
	return &target, nil
}

func (r *jobTaskRepository) SaveResult(ctx context.Context, write TaskResultWrite) (bool, error) {
	if write.TaskID <= 0 || write.InstanceID <= 0 || write.AttemptNo <= 0 ||
		write.ExecutorInstanceID.Version() != 4 || write.ExecutorInstanceID.Variant() != uuid.RFC4122 ||
		(write.Status != model.TaskSucceeded && write.Status != model.TaskRetryWaiting && write.Status != model.TaskFailed) ||
		(write.Status == model.TaskRetryWaiting && write.RetryIntervalSeconds <= 0) {
		return false, errors.New("invalid task result write")
	}
	if r.inTransaction() {
		return false, errors.New("task result save requires an independently committing database handle")
	}
	values := map[string]any{
		"status": write.Status, "result_data": nil, "result_summary": write.Summary,
		"finished_at": gorm.Expr("result_clock.processed_at"), "next_dispatch_at": nil,
		"end_reason": model.EndReasonExecutionFailed, "updated_at": gorm.Expr("result_clock.processed_at"),
	}
	switch write.Status {
	case model.TaskSucceeded:
		values["result_data"] = write.Data
		values["end_reason"] = model.EndReasonSuccess
	case model.TaskRetryWaiting:
		values["finished_at"], values["end_reason"] = nil, nil
		values["next_dispatch_at"] = gorm.Expr("result_clock.processed_at + ? * INTERVAL '1 second'", write.RetryIntervalSeconds)
	}
	// A volatile clock in a single-row FROM subquery is evaluated once for this
	// statement. Reusing it keeps all result times equal without an extra query
	// or the earlier JOIN/transaction timestamp. Row-lock waits may still follow.
	result := r.db.WithContext(ctx).Model(&model.JobTask{}).
		Clauses(clause.From{Tables: []clause.Table{{Name: "(SELECT clock_timestamp() AS processed_at)", Alias: "result_clock", Raw: true}}}).
		Where("job_task.id = ? AND job_task.instance_id = ? AND job_task.attempt_no = ? AND job_task.status = ?", write.TaskID, write.InstanceID, write.AttemptNo, model.TaskRunning).
		Where("job_task.executor_instance_id IS NULL OR job_task.executor_instance_id = ?", write.ExecutorInstanceID).
		Updates(values)
	if result.Error != nil {
		return false, fmt.Errorf("save job task result: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}

func (r *jobTaskRepository) CheckResultData(ctx context.Context, data model.TaskData) (bool, error) {
	if data == nil {
		return false, errors.New("result JSON data is absent")
	}
	var answer struct{ Valid *bool }
	// pg_input_is_valid supplies a result-content check without returning the
	// diagnostic message/detail that may contain the original business payload.
	result := r.db.WithContext(ctx).Raw("SELECT pg_catalog.pg_input_is_valid(?::text, 'pg_catalog.jsonb') AS valid", string(data)).Scan(&answer)
	if result.Error != nil {
		return false, fmt.Errorf("check job task result JSONB: %w", result.Error)
	}
	if result.RowsAffected != 1 || answer.Valid == nil {
		return false, errors.New("result JSONB validation returned no definite answer")
	}
	return *answer.Valid, nil
}

func (r *jobTaskRepository) inTransaction() bool {
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

func dispatchWriteError(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) == 5 {
		// These server responses reject the statement's data, integrity rules or
		// SQL/access preconditions. Do not infer permanence from arbitrary driver
		// errors: connection and transaction failures may leave commit uncertain.
		switch pgErr.Code[:2] {
		case "22", "23", "42":
			return fmt.Errorf("%s: %w: %w", operation, ErrDispatchWriteRejected, err)
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func validExecutorAddress(address string) bool {
	if !utf8.ValidString(address) || utf8.RuneCountInString(address) > 255 ||
		strings.IndexFunc(address, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return false
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || strings.ContainsAny(host, "/\\?#@") || net.JoinHostPort(host, port) != address {
		return false
	}
	// SplitHostPort checks bracket placement, not the contents of an IPv6 literal.
	if strings.Contains(host, ":") {
		ip, zone, _ := strings.Cut(host, "%")
		if net.ParseIP(ip) == nil || strings.Contains(host, "%") && zone == "" {
			return false
		}
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	number, err := strconv.ParseUint(port, 10, 16)
	return err == nil && number > 0
}
