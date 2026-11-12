// Package taskrecovery settles persisted attempts whose executor binding remains
// absent after observation. Settlement neither proves business failure nor grants
// permission to resend; any later attempt requires separate dispatch authorization.
package taskrecovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// MaxBatchSize bounds the parent/configuration read, not a transaction batch.
	MaxBatchSize = 16
	// DefaultObservationPeriod starts at the current attempt's database registration
	// time. All scheduler nodes must use the same configured observation period.
	DefaultObservationPeriod = 30 * time.Second
	// DefaultOperationTimeout includes connection waiting, execution and any commit
	// for each database call. It cannot extend an earlier caller deadline.
	DefaultOperationTimeout = time.Second
)

var (
	// ErrInvalidFacts reports unusable persisted identities or state. It must not
	// be treated as an ordinary missing candidate or permission to repair a Task.
	ErrInvalidFacts = errors.New("invalid unknown-settlement facts")
	// ErrInvalidConfig reports an unusable required frozen setting. The current
	// Job must not replace the snapshot, and the candidate remains unprocessed.
	ErrInvalidConfig = errors.New("invalid unknown-settlement configuration")
)

// databaseClock supplies fresh database time for one short batch. Context bounds
// the call, but cancellation does not prove the underlying request has returned.
type databaseClock interface {
	DatabaseTime(context.Context) (time.Time, error)
}

// taskStore reads a bounded set without explicit locks and independently commits
// each conditional settlement. FindUnboundTargets returns at most one projection
// per requested Task; missing rows are omitted. SettleUnknown returns a row
// only after its own commit is confirmed, nil, nil for no change, and an error for
// technical failure or an unknown commit outcome. It must not use an outer
// transaction, retry writes internally or infer a newer attempt from an old input.
type taskStore interface {
	FindUnboundTargets(context.Context, []int64) ([]repository.UnboundTarget, error)
	SettleUnknown(context.Context, repository.UnknownSettlement) (*repository.SettledTask, error)
}

// BatchResult describes a contiguous processed prefix of the original candidates.
// Processed includes verified skips, zero-row writes and a failed write that was
// actually attempted. It excludes failed reads, invalid required snapshots and
// work not started before cancellation. Applied counts only confirmed new commits.
// Callers retain both values even on error; neither proves all attempts succeeded.
type BatchResult struct {
	Processed int
	Applied   int
}

// Settler performs one bounded settlement batch without owning connections,
// goroutines, a queue or scan state. Each call takes fresh database time and reads
// its own target facts. Dependencies and optional hints must support concurrent
// calls; the host remains responsible for serializing its background scan loop.
type Settler struct {
	clock             databaseClock
	tasks             taskStore
	observationPeriod time.Duration
	operationTimeout  time.Duration
	hint              func(int64)
}

// Option configures a Settler before concurrent use begins.
type Option func(*Settler)

// WithObservationPeriod changes the positive binding observation period. It does
// not reset registration times or provide a business execution deadline.
func WithObservationPeriod(period time.Duration) Option {
	return func(s *Settler) { s.observationPeriod = period }
}

// WithOperationTimeout changes each database call's positive budget. The caller
// supplies the remaining whole-run/shutdown Context; no new run budget is created.
func WithOperationTimeout(timeout time.Duration) Option {
	return func(s *Settler) { s.operationTimeout = timeout }
}

// WithInstanceHint supplies a concurrent, nonblocking local check notification.
// It receives a parent after confirmed settlement or an observed stopping/expired
// state. It must enqueue a check of current facts rather than run it inline. Nil
// disables hints; this component does not implement stop, timeout or dispatch.
func WithInstanceHint(hint func(int64)) Option { return func(s *Settler) { s.hint = hint } }

// NewSettler binds dependencies without starting work or taking their ownership.
// The task store must confirm each write's independent commit before returning.
func NewSettler(clock databaseClock, tasks taskStore, options ...Option) (*Settler, error) {
	if nilDependency(clock) || nilDependency(tasks) {
		return nil, errors.New("unknown-settlement dependencies must not be nil")
	}
	s := &Settler{clock: clock, tasks: tasks, observationPeriod: DefaultObservationPeriod, operationTimeout: DefaultOperationTimeout}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("unknown-settlement option is nil")
		}
		option(s)
	}
	if s.observationPeriod <= 0 || s.operationTimeout <= 0 {
		return nil, errors.New("unknown-settlement periods must be positive")
	}
	return s, nil
}

// SettleBatch processes at most MaxBatchSize candidates in their original order.
// IDs and attempt numbers must be positive; original registration times must be
// nonzero. Those original times are pagination facts, never replacements for the
// current observation origin. An empty slice returns an empty result without I/O.
// The caller must not mutate candidates until this call actually returns.
//
// One fresh database time governs this batch's observation cutoff, parent deadline
// checks and retry times. One bounded read obtains all targets; each eligible Task
// then uses its own conditional write and commit. No explicit lock, RPC, immediate
// retry or parent transition is performed here. A failed write ends the batch after
// counting that attempted item; failed reads or invalid required settings cannot
// advance past the affected item. Earlier confirmed changes remain in Applied.
// All four Task stages use this path. The frozen execution mode must match the
// persisted stage, and a Post Task requires Post to be enabled in that snapshot.
//
// Cancellation stops new operations but does not prove rollback or release of a
// database resource. The caller keeps its processing position until actual return.
// A confirmed late commit still counts and emits a hint; an expired operation then
// ends the batch. Errors may retain storage or JSON diagnostics, so hosts must log
// safe classifications and IDs rather than raw errors or frozen configuration.
func (s *Settler) SettleBatch(ctx context.Context, candidates []repository.UnboundCandidate) (BatchResult, error) {
	var result BatchResult
	if len(candidates) > MaxBatchSize {
		return result, errors.New("unknown-settlement batch exceeds its limit")
	}
	if len(candidates) == 0 {
		return result, nil
	}
	ids := make([]int64, len(candidates))
	requested := make(map[int64]struct{}, len(candidates))
	for index, candidate := range candidates {
		if candidate.TaskID <= 0 || candidate.InstanceID <= 0 || candidate.AttemptNo <= 0 || candidate.AttemptRegisteredAt.IsZero() {
			return result, errors.New("invalid unknown-settlement candidate")
		}
		ids[index] = candidate.TaskID
		requested[candidate.TaskID] = struct{}{}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	opCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	processedAt, err := s.clock.DatabaseTime(opCtx)
	if err == nil {
		err = opCtx.Err()
	}
	cancel()
	if err != nil {
		return result, fmt.Errorf("read unknown-settlement time: %w", err)
	}
	if processedAt.IsZero() {
		return result, ErrInvalidFacts
	}
	opCtx, cancel = context.WithTimeout(ctx, s.operationTimeout)
	targets, err := s.tasks.FindUnboundTargets(opCtx, ids)
	if err == nil {
		err = opCtx.Err()
	}
	cancel()
	if err != nil {
		return result, fmt.Errorf("read unknown-settlement targets: %w", err)
	}
	byID := make(map[int64]repository.UnboundTarget, len(targets))
	for _, target := range targets {
		if _, ok := requested[target.TaskID]; !ok {
			return result, ErrInvalidFacts
		}
		if _, duplicate := byID[target.TaskID]; duplicate {
			return result, ErrInvalidFacts
		}
		byID[target.TaskID] = target
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		target, found := byID[candidate.TaskID]
		if !found {
			result.Processed++
			continue
		}
		write, eligible, err := s.prepare(ctx, candidate, target, processedAt)
		if err != nil {
			return result, err
		}
		if !eligible {
			result.Processed++
			continue
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		opCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
		settled, err := s.tasks.SettleUnknown(opCtx, write)
		operationErr := opCtx.Err()
		cancel()
		result.Processed++
		if err != nil {
			return result, fmt.Errorf("settle unknown task %d: %w", candidate.TaskID, err)
		}
		if settled != nil {
			if settled.TaskID != candidate.TaskID || settled.InstanceID != candidate.InstanceID || settled.AttemptNo != candidate.AttemptNo ||
				(settled.Status != model.TaskRetryWaiting && settled.Status != model.TaskFailed) {
				return result, ErrInvalidFacts
			}
			result.Applied++
			s.hintInstance(candidate.InstanceID)
		}
		if operationErr != nil {
			return result, fmt.Errorf("unknown-settlement operation expired: %w", operationErr)
		}
	}
	return result, nil
}

func (s *Settler) prepare(ctx context.Context, candidate repository.UnboundCandidate, target repository.UnboundTarget, processedAt time.Time) (repository.UnknownSettlement, bool, error) {
	var empty repository.UnknownSettlement
	// Disqualify obsolete work before interpreting a snapshot it no longer needs.
	if target.InstanceID != candidate.InstanceID || target.AttemptNo != candidate.AttemptNo {
		return empty, false, nil
	}
	switch target.TaskType {
	case model.TaskTypeOrdinary, model.TaskTypePrepare, model.TaskTypeShard, model.TaskTypePost:
	default:
		return empty, false, ErrInvalidFacts
	}
	switch target.Status {
	case model.TaskRunning:
	case model.TaskWaiting, model.TaskRetryWaiting, model.TaskSucceeded, model.TaskFailed, model.TaskCanceled:
		return empty, false, nil
	default:
		return empty, false, ErrInvalidFacts
	}
	if target.ExecutorInstanceID != nil || target.ExecutorAddress != nil {
		return empty, false, nil
	}
	cutoff := processedAt.Add(-s.observationPeriod)
	if target.AttemptRegisteredAt == nil || target.AttemptRegisteredAt.IsZero() {
		diagnoseSkipped(ctx, candidate, "invalid_attempt_registration_time")
		return empty, false, nil
	}
	if target.AttemptRegisteredAt.After(cutoff) {
		return empty, false, nil
	}
	switch target.InstanceStatus {
	case model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled:
		return empty, false, nil
	case model.InstanceStopping:
		s.hintInstance(candidate.InstanceID)
		return empty, false, nil
	case model.InstanceWaiting:
		diagnoseSkipped(ctx, candidate, "parent_not_started")
		return empty, false, nil
	case model.InstanceRunning:
	default:
		return empty, false, ErrInvalidFacts
	}
	if target.StartedAt == nil || target.StartedAt.IsZero() || target.DeadlineAt == nil || !target.DeadlineAt.After(*target.StartedAt) {
		diagnoseSkipped(ctx, candidate, "invalid_parent_execution_times")
		return empty, false, nil
	}
	if !processedAt.Before(*target.DeadlineAt) {
		s.hintInstance(candidate.InstanceID)
		return empty, false, nil
	}
	maxAttempts, retryInterval, err := frozenBudget(target.ExecutionConfig, target.TaskType)
	if err != nil {
		return empty, false, err
	}
	return repository.UnknownSettlement{TaskID: candidate.TaskID, InstanceID: candidate.InstanceID, ExpectedAttemptNo: candidate.AttemptNo,
		MaxAttempts: maxAttempts, ProcessedAt: processedAt, ObservationCutoff: cutoff, RetryAt: processedAt.Add(retryInterval)}, true, nil
}

func frozenBudget(data []byte, taskType model.TaskType) (int64, time.Duration, error) {
	var config struct {
		SchemaVersion int32               `json:"schema_version"`
		ExecutionMode model.ExecutionMode `json:"execution_mode"`
		PostEnabled   *bool               `json:"post_enabled"`
		MaxRetries    json.RawMessage     `json:"max_retries"`
		RetryInterval json.RawMessage     `json:"retry_interval_seconds"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return 0, 0, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	if config.SchemaVersion != 1 || config.PostEnabled == nil {
		return 0, 0, ErrInvalidConfig
	}
	switch taskType {
	case model.TaskTypeOrdinary:
		if config.ExecutionMode != model.ExecutionModeNormal || *config.PostEnabled {
			return 0, 0, ErrInvalidConfig
		}
	case model.TaskTypePrepare, model.TaskTypeShard:
		if config.ExecutionMode != model.ExecutionModeSharded {
			return 0, 0, ErrInvalidConfig
		}
	case model.TaskTypePost:
		if config.ExecutionMode != model.ExecutionModeSharded || !*config.PostEnabled {
			return 0, 0, ErrInvalidConfig
		}
	default:
		return 0, 0, ErrInvalidFacts
	}
	var retries, interval *int32
	if json.Unmarshal(config.MaxRetries, &retries) != nil || json.Unmarshal(config.RetryInterval, &interval) != nil ||
		retries == nil || interval == nil || *retries < 0 || *interval <= 0 {
		return 0, 0, ErrInvalidConfig
	}
	// The UPDATE decides the branch from its current used_attempts; no stale
	// budget counter is read here. Widen before including the first opportunity.
	return int64(*retries) + 1, time.Duration(*interval) * time.Second, nil
}

func (s *Settler) hintInstance(id int64) {
	if s.hint != nil {
		s.hint(id)
	}
}

func diagnoseSkipped(ctx context.Context, candidate repository.UnboundCandidate, reason string) {
	logx.WithContext(ctx).Errorw("job task unknown settlement skipped", logx.Field("task_id", candidate.TaskID),
		logx.Field("instance_id", candidate.InstanceID), logx.Field("attempt_no", candidate.AttemptNo), logx.Field("reason", reason))
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
