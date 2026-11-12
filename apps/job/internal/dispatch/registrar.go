// Package dispatch coordinates persisted execution registration before sending
// work to executors. Registration alone does not send an RPC or run business code.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

const registrationTimeout = 2 * time.Second

var (
	// ErrInvalidInstanceState reports inconsistent persisted execution facts.
	// Registration must stop without repairing or granting a new attempt.
	ErrInvalidInstanceState = errors.New("invalid instance execution state")
	// ErrInvalidExecutionConfig reports an unusable frozen execution snapshot.
	// The current Job must not replace it or supply missing values.
	ErrInvalidExecutionConfig = errors.New("invalid instance execution configuration")
)

// RegisteredAttempt describes a newly registered, committed dispatch attempt.
// ExecutionConfig and the group come from the Instance's frozen snapshot, not
// the current Job. RegisteredAt is database registration time, not business start
// time. DeadlineAt is the round deadline and must not be extended on dispatch.
// InputData is the same Task's fixed shard value or Post statistics returned by
// registration, copied out of repository-owned storage. Ordinary and prepare
// attempts carry nil input. Sending must not query a replacement input later.
//
// Only the call that creates this attempt may send it, while its original dispatch
// budget remains. Reconstructing this value from a later query does not authorize
// resending the attempt. A lost value or expired budget is handled by recovery;
// it must not cause an immediate resend or a refund of the occupied opportunity.
type RegisteredAttempt struct {
	JobID           int64
	InstanceID      int64
	ExecutorGroupID int64
	TaskID          int64
	TaskType        model.TaskType
	AttemptNo       int64
	InputData       model.TaskData
	ExecutionConfig model.ExecutionConfig
	RegisteredAt    time.Time
	DeadlineAt      time.Time
}

// Registrar coordinates short execution-registration transactions. It owns no
// connection, queue or goroutine and may be used concurrently when its runner
// supports concurrent calls. The host owns the runner and its database lifetime.
type Registrar struct {
	transact repository.TransactionRunner
	hint     func(int64)
}

// RegistrarOption configures a Registrar before concurrent use.
type RegistrarOption func(*Registrar)

// WithRegistrationHint supplies a concurrent, nonblocking parent-check hint.
// A due Task whose parent is stopping or expired calls it only after the transaction
// ends successfully. The hint does not decide timeout, authorize another attempt
// or guarantee that recovery runs. Nil disables notifications.
func WithRegistrationHint(hint func(int64)) RegistrarOption {
	return func(r *Registrar) { r.hint = hint }
}

// NewRegistrar binds a transaction runner without querying the database. The
// runner must confirm commit before returning nil; a callback's success alone
// must not be reported as a committed transaction.
func NewRegistrar(transact repository.TransactionRunner, options ...RegistrarOption) (*Registrar, error) {
	if transact == nil {
		return nil, errors.New("dispatch transaction runner is nil")
	}
	r := &Registrar{transact: transact}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("registration option is nil")
		}
		option(r)
	}
	return r, nil
}

// StartInstance atomically starts a waiting Instance and creates its ordinary or
// prepare Task in the running state with attempt number and used attempts both one.
// The caller must already hold dispatch capacity and pass its remaining overall
// processing budget. This method bounds the entire transaction, including commit,
// to two seconds or that earlier deadline; cancellation is not proof of rollback.
// Capacity remains the caller's responsibility until the call actually returns.
//
// A missing Instance or an Instance no longer waiting returns nil, nil.
// A NOWAIT conflict preserves repository.ErrInstanceLocked after rollback;
// the caller must skip the remaining candidates in that parent's current group.
// Neither case permits immediate requeueing or looking up an old Task to resend.
// An inconsistent waiting record or invalid snapshot returns an error without repair.
// Only a newly created Task whose commit is confirmed produces a non-nil result.
// Every error returns nil, including a lost commit confirmation after the runner
// has already assigned a Task ID. No RPC or post-commit notification is sent here.
func (r *Registrar) StartInstance(ctx context.Context, instanceID int64) (*RegisteredAttempt, error) {
	if instanceID <= 0 {
		return nil, errors.New("dispatch instance ID must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, registrationTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var registered *RegisteredAttempt
	err := r.transact(ctx, func(repos repository.Repositories) error {
		instance, err := repos.Instances.LockForExecution(ctx, instanceID)
		if err != nil {
			return err
		}
		if instance == nil || instance.Status != model.InstanceWaiting {
			return nil
		}
		if instance.ID != instanceID || instance.StartedAt != nil || instance.DeadlineAt != nil || instance.FinishedAt != nil {
			return ErrInvalidInstanceState
		}
		// Decode only after the state check. Old candidates must be skipped even
		// if their no-longer-needed snapshot cannot be decoded.
		config, err := decodeConfig(instance.ExecutionConfig)
		if err != nil {
			return err
		}
		taskType := model.TaskTypeOrdinary
		if config.ExecutionMode == model.ExecutionModeSharded {
			taskType = model.TaskTypePrepare
		}
		if instance.JobID <= 0 || instance.ExecutorGroupID <= 0 {
			return ErrInvalidInstanceState
		}
		startedAt, err := repos.Instances.DatabaseTime(ctx)
		if err != nil {
			return err
		}
		if startedAt.IsZero() {
			return errors.New("database returned an empty execution start time")
		}
		deadlineAt := startedAt.Add(time.Duration(config.TimeoutSeconds) * time.Second)
		changed, err := repos.Instances.StartExecution(ctx, instanceID, startedAt, deadlineAt)
		if err != nil {
			return err
		}
		if !changed {
			// The parent was locked and validated by this transaction. A missed
			// conditional update is an invariant failure, not a normal replay.
			return ErrInvalidInstanceState
		}
		taskID, err := repos.Tasks.InsertInitial(ctx, instanceID, taskType, startedAt)
		if err != nil {
			return err
		}
		if taskID <= 0 {
			return errors.New("task repository returned an invalid inserted ID")
		}
		registered = &RegisteredAttempt{
			JobID: instance.JobID, InstanceID: instanceID, ExecutorGroupID: instance.ExecutorGroupID,
			TaskID: taskID, TaskType: taskType, AttemptNo: 1, ExecutionConfig: config,
			RegisteredAt: startedAt, DeadlineAt: deadlineAt,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("register instance %d: %w", instanceID, err)
	}
	return registered, nil
}

// RegisterTask registers the first pending shard/Post attempt or the next retry under
// its parent's NOWAIT lock. The caller already owns dispatch capacity; registration
// is bounded by two seconds and the caller's remaining budget. Only a newly
// returned Task version with confirmed commit authorizes a send. An old candidate,
// exhausted budget or changed state is never replaced with a newer attempt lookup.
// A lost commit confirmation grants no permission, even if the UPDATE returned a row.
//
// Missing or inactive parents and zero-row updates return nil, nil.
// Stopping or expired running parents additionally receive the optional hint after
// the transaction ends successfully. Their state is not changed here. Lock conflicts
// preserve repository.ErrInstanceLocked after rollback so the caller can skip the
// remaining candidates in the same parent's current group. Other errors preserve
// their causes. Every error returns nil; this method never sends or refunds work.
func (r *Registrar) RegisterTask(ctx context.Context, candidate repository.DispatchCandidate) (*RegisteredAttempt, error) {
	if !validDispatchCandidate(candidate) {
		return nil, errors.New("dispatch candidate identity or state is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, registrationTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var registered *RegisteredAttempt
	var checkParent bool
	err := r.transact(ctx, func(repos repository.Repositories) error {
		instance, err := repos.Instances.LockForExecution(ctx, candidate.InstanceID)
		if err != nil {
			// A NOWAIT failure aborts the transaction. Let the runner roll back
			// before the caller classifies it and skips this parent's candidates.
			return err
		}
		if instance == nil {
			return nil
		}
		if instance.ID != candidate.InstanceID {
			return ErrInvalidInstanceState
		}
		if instance.Status != model.InstanceRunning {
			checkParent = instance.Status == model.InstanceStopping
			return nil
		}
		registeredAt, err := repos.Instances.DatabaseTime(ctx)
		if err != nil {
			return err
		}
		if registeredAt.IsZero() || instance.StartedAt == nil || instance.StartedAt.IsZero() ||
			instance.DeadlineAt == nil || !instance.DeadlineAt.After(*instance.StartedAt) ||
			instance.FinishedAt != nil || instance.JobID <= 0 || instance.ExecutorGroupID <= 0 {
			return ErrInvalidInstanceState
		}
		if !registeredAt.Before(*instance.DeadlineAt) {
			checkParent = true
			return nil
		}
		config, err := decodeConfig(instance.ExecutionConfig)
		if err != nil {
			return err
		}
		if !configAllowsTask(config, candidate.TaskType) {
			return ErrInvalidInstanceState
		}
		// Widen before adding the first opportunity. Attempt numbers can exceed
		// this budget because explicit BUSY responses refund used attempts.
		maxAttempts := int64(config.MaxRetries) + 1
		task, err := repos.Tasks.RegisterAttempt(ctx, repository.AttemptRegistration{
			TaskID: candidate.TaskID, InstanceID: candidate.InstanceID, ExpectedAttemptNo: candidate.AttemptNo,
			TaskType: candidate.TaskType, ExpectedStatus: candidate.Status,
			MaxAttempts: maxAttempts, RegisteredAt: registeredAt,
		})
		if err != nil || task == nil {
			return err
		}
		if task.TaskID != candidate.TaskID || task.InstanceID != candidate.InstanceID || task.TaskType != candidate.TaskType ||
			task.AttemptNo <= candidate.AttemptNo || task.AttemptNo-candidate.AttemptNo != 1 ||
			task.UsedAttempts <= 0 || int64(task.UsedAttempts) > maxAttempts || !task.RegisteredAt.Equal(registeredAt) {
			return ErrInvalidInstanceState
		}
		registered = &RegisteredAttempt{
			JobID: instance.JobID, InstanceID: instance.ID, ExecutorGroupID: instance.ExecutorGroupID,
			TaskID: task.TaskID, TaskType: task.TaskType, AttemptNo: task.AttemptNo, ExecutionConfig: config,
			InputData:    append(model.TaskData(nil), task.InputData...),
			RegisteredAt: task.RegisteredAt, DeadlineAt: *instance.DeadlineAt,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("register task %d: %w", candidate.TaskID, err)
	}
	if checkParent && r.hint != nil {
		r.hint(candidate.InstanceID)
	}
	return registered, nil
}

func decodeConfig(data []byte) (model.ExecutionConfig, error) {
	// Missing or null zero-valued fields must not become valid defaults. Every
	// snapshot member is written at creation, including false and empty values.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return model.ExecutionConfig{}, fmt.Errorf("%w: %w", ErrInvalidExecutionConfig, err)
	}
	for _, name := range []string{"schema_version", "execution_mode", "processor_name", "params", "post_enabled", "max_retries", "retry_interval_seconds", "timeout_seconds"} {
		value, present := members[name]
		if !present || strings.TrimSpace(string(value)) == "null" {
			return model.ExecutionConfig{}, ErrInvalidExecutionConfig
		}
	}
	var config model.ExecutionConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return model.ExecutionConfig{}, fmt.Errorf("%w: %w", ErrInvalidExecutionConfig, err)
	}
	if config.SchemaVersion != 1 {
		return model.ExecutionConfig{}, ErrInvalidExecutionConfig
	}
	if (config.ExecutionMode != model.ExecutionModeNormal && config.ExecutionMode != model.ExecutionModeSharded) ||
		(config.ExecutionMode == model.ExecutionModeNormal && config.PostEnabled) ||
		config.MaxRetries < 0 || config.RetryIntervalSeconds <= 0 || config.TimeoutSeconds <= 0 ||
		strings.TrimSpace(config.ProcessorName) == "" || config.ProcessorName != strings.TrimSpace(config.ProcessorName) || utf8.RuneCountInString(config.ProcessorName) > 128 ||
		!utf8.ValidString(config.ProcessorName) || strings.ContainsRune(config.ProcessorName, 0) ||
		!utf8.ValidString(config.Params) || strings.ContainsRune(config.Params, 0) {
		return model.ExecutionConfig{}, ErrInvalidExecutionConfig
	}
	return config, nil
}

func validDispatchCandidate(candidate repository.DispatchCandidate) bool {
	if candidate.TaskID <= 0 || candidate.InstanceID <= 0 {
		return false
	}
	switch candidate.Status {
	case model.TaskWaiting:
		return candidate.AttemptNo == 0 && (candidate.TaskType == model.TaskTypeShard || candidate.TaskType == model.TaskTypePost)
	case model.TaskRetryWaiting:
		return candidate.AttemptNo > 0 && candidate.TaskType >= model.TaskTypeOrdinary && candidate.TaskType <= model.TaskTypePost
	default:
		return false
	}
}

func configAllowsTask(config model.ExecutionConfig, taskType model.TaskType) bool {
	if config.ExecutionMode == model.ExecutionModeNormal {
		return taskType == model.TaskTypeOrdinary
	}
	return taskType == model.TaskTypePrepare || taskType == model.TaskTypeShard || (taskType == model.TaskTypePost && config.PostEnabled)
}
