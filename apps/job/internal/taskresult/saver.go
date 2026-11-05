package taskresult

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

// DefaultSaveTimeout covers one result's lookup, conditional write, commit and
// any exceptional content validation. Queue waiting and later backoff are separate.
const DefaultSaveTimeout = 2 * time.Second

var (
	// ErrInvalidStoredState identifies a platform invariant failure, not evidence
	// that the executor's report is permanently invalid. Retain the report.
	ErrInvalidStoredState = errors.New("invalid persisted task execution state")
	// ErrInvalidExecutionConfig requires repairing the frozen platform snapshot;
	// the current Job must not replace it, and the report must be retained.
	ErrInvalidExecutionConfig = errors.New("invalid task result execution configuration")
	// ErrUnsupportedTaskType preserves reports for stages this saver cannot yet
	// process. It must not be translated into successful ordinary-task persistence.
	ErrUnsupportedTaskType = errors.New("task result type is not supported by this saver")
)

// SaveResult describes this processing attempt, not an RPC acknowledgement or an
// Instance outcome. A nil error from Save permits releasing the report; Applied
// is true only when this call confirmed a new write. Reason is a diagnostic code,
// not a business subcode. A skipped report never proves an earlier write succeeded.
type SaveResult struct {
	Applied bool
	Reason  string
}

// Saver validates and independently persists one ordinary Task result at a time.
// It has no queue, retry loop or goroutine. Use a non-transaction repository: its
// SaveResult must confirm the conditional write's own commit before returning.
// The host owns the repository and its database, and must retain reports whenever
// Save returns an error. Saver may be called concurrently when its dependencies
// and optional hint callback support concurrent use.
type Saver struct {
	tasks   repository.JobTaskRepository
	timeout time.Duration
	hint    func(int64)
}

// Option configures a Saver during construction, never after concurrent use starts.
type Option func(*Saver)

// WithSaveTimeout changes the whole per-result processing budget. It must be
// positive and cannot extend an earlier caller or shared shutdown deadline.
func WithSaveTimeout(timeout time.Duration) Option {
	return func(s *Saver) { s.timeout = timeout }
}

// WithInstanceHint supplies a concurrent, nonblocking local notification callback.
// It receives only verified parent IDs after a confirmed write or an obsolete
// report. It must read current database facts, not advance from the old report.
// A nil callback disables hints; missing hints are covered by normal recovery.
func WithInstanceHint(hint func(int64)) Option { return func(s *Saver) { s.hint = hint } }

// NewSaver binds a repository without taking ownership of its connections or
// starting background work. It rejects nil dependencies and nonpositive budgets.
func NewSaver(tasks repository.JobTaskRepository, options ...Option) (*Saver, error) {
	if nilTaskRepository(tasks) {
		return nil, errors.New("task result repository is nil")
	}
	s := &Saver{tasks: tasks, timeout: DefaultSaveTimeout}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("task result option is nil")
		}
		option(s)
	}
	if s.timeout <= 0 {
		return nil, errors.New("task result save timeout must be positive")
	}
	return s, nil
}

// Save processes one credential-free report under a single budget, using its
// original identities and trusted GroupID. The caller supplies a worker Context,
// not the already-completed inbound RPC's Context, and must not mutate the report's
// slices until the call returns. Failed business work ignores Data and PreparedItems.
//
// A nil error means applied, obsolete, or independently invalid; inspect Applied
// and Reason to distinguish them. All errors require retaining this same report,
// including technical failures, unusable platform snapshots and uncertain commits.
// An error always returns a zero SaveResult and sends no hint. Database errors
// retain their cause chains, which may contain driver diagnostics: callers must
// not log raw errors, summaries or result bytes. Log IDs and safe classifications.
//
// Parent state and deadline do not veto an otherwise current Task result. Saving
// does not restore the parent, grant another attempt or start Post processing.
func (s *Saver) Save(ctx context.Context, report Report) (SaveResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return SaveResult{}, err
	}
	identity, err := uuid.Parse(report.ExecutorInstanceID)
	if report.GroupID <= 0 || report.JobID <= 0 || report.InstanceID <= 0 || report.TaskID <= 0 || report.AttemptNo <= 0 ||
		err != nil || identity.Version() != 4 || identity.Variant() != uuid.RFC4122 || identity.String() != report.ExecutorInstanceID ||
		(report.Outcome != OutcomeSuccess && report.Outcome != OutcomeFailure) {
		return discardResult(ctx, report, "invalid_report"), nil
	}
	target, err := s.tasks.FindResultTarget(ctx, report.TaskID)
	if err != nil {
		return SaveResult{}, fmt.Errorf("read task result target: %w", err)
	}
	if target == nil {
		return discardResult(ctx, report, "missing_target"), nil
	}
	if target.TaskID != report.TaskID || target.InstanceID != report.InstanceID || target.JobID != report.JobID || target.ExecutorGroupID != report.GroupID {
		return discardResult(ctx, report, "ownership_mismatch"), nil
	}
	// These facts can disqualify an obsolete report without using its parent's
	// configuration. A damaged snapshot must not retain an already-closed attempt.
	if target.AttemptNo != report.AttemptNo {
		return s.finished(target.InstanceID, false, "stale_attempt"), nil
	}
	switch target.Status {
	case model.TaskRunning:
	case model.TaskWaiting, model.TaskRetryWaiting, model.TaskSucceeded, model.TaskFailed, model.TaskCanceled:
		return s.finished(target.InstanceID, false, "task_closed"), nil
	default:
		return SaveResult{}, ErrInvalidStoredState
	}
	if target.ExecutorInstanceID != nil && *target.ExecutorInstanceID != identity {
		return s.finished(target.InstanceID, false, "executor_mismatch"), nil
	}
	if target.TaskType != model.TaskTypeOrdinary {
		return SaveResult{}, ErrUnsupportedTaskType
	}
	if !utf8.ValidString(report.Summary) || strings.ContainsRune(report.Summary, 0) ||
		(report.Outcome == OutcomeSuccess && (report.PreparedItems != nil ||
			(report.Data != nil && (!utf8.Valid(report.Data) || !json.Valid(report.Data))))) {
		return discardResult(ctx, report, "invalid_output"), nil
	}
	write, err := resultWrite(target, report, identity)
	if err != nil {
		return SaveResult{}, err
	}
	applied, err := s.tasks.SaveResult(ctx, write)
	if err != nil {
		// A data-exception SQLSTATE only selects an exceptional validation path;
		// it is not evidence that this report is invalid. The failed write has
		// already left its transaction. Probe only the actual successful Data,
		// using the remaining budget and the database's own JSONB input rules.
		var pgErr *pgconn.PgError
		if !applied && ctx.Err() == nil && report.Outcome == OutcomeSuccess && report.Data != nil &&
			errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
			valid, validationErr := s.tasks.CheckResultData(ctx, model.TaskResultData(report.Data))
			if validationErr == nil && !valid {
				return discardResult(ctx, report, "invalid_output"), nil
			}
			if validationErr != nil {
				return SaveResult{}, fmt.Errorf("save task result: %w; validate result data: %w", err, validationErr)
			}
		}
		return SaveResult{}, fmt.Errorf("save task result: %w", err)
	}
	if !applied {
		return s.finished(target.InstanceID, false, "no_match"), nil
	}
	return s.finished(target.InstanceID, true, "saved"), nil
}

func (s *Saver) finished(instanceID int64, applied bool, reason string) SaveResult {
	if s.hint != nil {
		s.hint(instanceID)
	}
	return SaveResult{Applied: applied, Reason: reason}
}

func discardResult(ctx context.Context, report Report, reason string) SaveResult {
	logx.WithContext(ctx).Errorw("job task result discarded",
		logx.Field("job_id", report.JobID), logx.Field("instance_id", report.InstanceID),
		logx.Field("task_id", report.TaskID), logx.Field("attempt_no", report.AttemptNo),
		logx.Field("reason", reason))
	return SaveResult{Reason: reason}
}

func resultWrite(target *repository.TaskResultTarget, report Report, identity uuid.UUID) (repository.TaskResultWrite, error) {
	// Decode only fields this operation needs. Successful results do not need
	// retry settings, and unrelated processor parameters are never interpreted.
	var config struct {
		SchemaVersion int32               `json:"schema_version"`
		ExecutionMode model.ExecutionMode `json:"execution_mode"`
		MaxRetries    json.RawMessage     `json:"max_retries"`
		RetryInterval json.RawMessage     `json:"retry_interval_seconds"`
	}
	if err := json.Unmarshal(target.ExecutionConfig, &config); err != nil {
		return repository.TaskResultWrite{}, fmt.Errorf("%w: %w", ErrInvalidExecutionConfig, err)
	}
	if config.SchemaVersion != 1 || config.ExecutionMode != model.ExecutionModeNormal {
		return repository.TaskResultWrite{}, ErrInvalidExecutionConfig
	}
	write := repository.TaskResultWrite{TaskID: report.TaskID, InstanceID: report.InstanceID,
		AttemptNo: report.AttemptNo, ExecutorInstanceID: identity, Summary: report.Summary}
	if report.Outcome == OutcomeSuccess {
		write.Status, write.Data = model.TaskSucceeded, model.TaskResultData(report.Data)
		return write, nil
	}
	var maxRetries, interval *int32
	if json.Unmarshal(config.MaxRetries, &maxRetries) != nil || json.Unmarshal(config.RetryInterval, &interval) != nil ||
		maxRetries == nil || interval == nil || *maxRetries < 0 || *interval <= 0 {
		return repository.TaskResultWrite{}, ErrInvalidExecutionConfig
	}
	if target.UsedAttempts <= 0 {
		return repository.TaskResultWrite{}, ErrInvalidStoredState
	}
	// Convert before adding the first opportunity; MaxInt32 retries must not
	// wrap to a negative allowance. This operation never increments the counter.
	if int64(target.UsedAttempts) < int64(*maxRetries)+1 {
		write.Status, write.RetryIntervalSeconds = model.TaskRetryWaiting, *interval
	} else {
		write.Status = model.TaskFailed
	}
	return write, nil
}

func nilTaskRepository(tasks repository.JobTaskRepository) bool {
	if tasks == nil {
		return true
	}
	value := reflect.ValueOf(tasks)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
