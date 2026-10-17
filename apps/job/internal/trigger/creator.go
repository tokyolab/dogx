package trigger

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/schedule"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	DefaultCreationCapacity = 32
	DefaultCreationBudget   = 3 * time.Second
	MisfireTolerance        = 5 * time.Second
)

// Creator coordinates the automatic/manual instance-creation transaction.
// Sharing one Creator keeps both entry points within the same bounded capacity.
type Creator struct {
	transact repository.TransactionRunner
	now      func() time.Time
	hint     func(int64)
	capacity int
	budget   time.Duration
	slots    chan struct{}
}

type Option func(*Creator)

func WithClock(now func() time.Time) Option { return func(s *Creator) { s.now = now } }

// The hint consumer must be nonblocking; persistence, not this hint, is the handoff.
func WithInstanceHint(hint func(int64)) Option { return func(s *Creator) { s.hint = hint } }

func WithCreationLimits(capacity int, budget time.Duration) Option {
	return func(s *Creator) { s.capacity, s.budget = capacity, budget }
}

func New(transact repository.TransactionRunner, options ...Option) (*Creator, error) {
	if transact == nil {
		return nil, errors.New("job trigger transaction runner is nil")
	}
	s := &Creator{
		transact: transact,
		now:      time.Now,
		capacity: DefaultCreationCapacity,
		budget:   DefaultCreationBudget,
	}
	for _, option := range options {
		option(s)
	}
	if s.now == nil || s.capacity <= 0 || s.budget <= 0 {
		return nil, errors.New("invalid job trigger clock or creation limits")
	}
	s.slots = make(chan struct{}, s.capacity)
	return s, nil
}

type ManualTrigger struct {
	JobID            int64
	RequestID        uuid.UUID
	ParamsOverridden bool
	Params           string
	OperatorID       int64
}

func (s *Creator) TriggerManual(ctx context.Context, input ManualTrigger) (int64, error) {
	if input.JobID <= 0 || input.OperatorID <= 0 || (!input.ParamsOverridden && input.Params != "") {
		return 0, ErrInvalidInput
	}
	var err error
	input.Params, err = normalizeParams(input.Params)
	if err != nil {
		return 0, err
	}
	ctx, release, err := s.acquire(ctx, false)
	if err != nil {
		return 0, err
	}
	defer release()
	var id int64
	var created bool
	err = s.transact(ctx, func(r repository.Repositories) error {
		job, err := r.Jobs.Lock(ctx, input.JobID)
		if err != nil {
			return err
		}
		if job == nil {
			return errors.New("job repository returned nil definition without error")
		}
		prior, err := r.Instances.FindManual(ctx, input.JobID, input.RequestID)
		if err != nil {
			return err
		}
		if prior != nil {
			id, err = replay(prior, input)
			return err
		}
		if err := availableGroup(ctx, r, job.ExecutorGroupID); err != nil {
			return err
		}
		if job.BlockPolicy == model.BlockDiscard {
			active, err := r.Instances.HasActive(ctx, job.ID)
			if err != nil {
				return err
			}
			if active {
				return ErrBlocked
			}
		}
		instance := snapshot(job)
		instance.TriggerType = model.TriggerManual
		instance.RequestID = &input.RequestID
		instance.ParamsOverridden = input.ParamsOverridden
		instance.TriggeredBy = &input.OperatorID
		if input.ParamsOverridden {
			instance.ExecutionConfig.Params = input.Params
		}
		id, created, err = r.Instances.Insert(ctx, instance)
		if err != nil || created {
			return err
		}
		// A conflict's tuple can become visible only to a new READ COMMITTED
		// statement. Never combine INSERT and fallback SELECT into one CTE.
		prior, err = r.Instances.FindManual(ctx, input.JobID, input.RequestID)
		if err != nil {
			return err
		}
		if prior == nil {
			return ErrIndeterminate
		}
		id, err = replay(prior, input)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("create manual instance for job %d: %w", input.JobID, err)
	}
	if created && s.hint != nil {
		s.hint(id)
	}
	return id, nil
}

type AutomaticResult struct {
	InstanceID int64
	Created    bool
	Reason     string
}

func (s *Creator) TriggerAutomatic(ctx context.Context, id int64) (AutomaticResult, error) {
	result := AutomaticResult{}
	if id <= 0 {
		return result, ErrInvalidInput
	}
	ctx, release, err := s.acquire(ctx, true)
	if err != nil {
		return result, err
	}
	defer release()
	var scheduledAt, decisionAt, nextTriggerAt time.Time
	err = s.transact(ctx, func(r repository.Repositories) error {
		job, err := r.Jobs.Lock(ctx, id)
		if errors.Is(err, repository.ErrJobNotFound) {
			result.Reason = "not_found"
			return nil
		}
		if err != nil {
			return err
		}
		if job == nil {
			return errors.New("job repository returned nil definition without error")
		}
		decisionAt = s.now()
		if job.Status != basemodel.RecordStatusEnabled {
			result.Reason = "disabled"
			return nil
		}
		if job.NextTriggerAt == nil || job.NextTriggerAt.After(decisionAt) {
			result.Reason = "not_due"
			return nil
		}
		next, err := schedule.Next(job.CronExpr, decisionAt)
		if err != nil {
			return err
		}
		scheduledAt, nextTriggerAt = *job.NextTriggerAt, next
		triggerType := model.TriggerScheduled
		if decisionAt.Sub(*job.NextTriggerAt) > MisfireTolerance {
			triggerType = model.TriggerMisfire
			if job.MisfirePolicy == model.MisfireIgnore {
				result.Reason = "misfire_ignored"
			}
		}
		if result.Reason == "" && job.BlockPolicy == model.BlockDiscard {
			active, err := r.Instances.HasActive(ctx, id)
			if err != nil {
				return err
			}
			if active {
				result.Reason = "blocked"
			}
		}
		if result.Reason == "" {
			instance := snapshot(job)
			instance.TriggerType, instance.ScheduledAt = triggerType, job.NextTriggerAt
			result.InstanceID, result.Created, err = r.Instances.Insert(ctx, instance)
			if err != nil {
				return err
			}
			if result.Created {
				result.Reason = "created"
			} else {
				result.Reason = "duplicate"
			}
		}
		return r.Jobs.Advance(ctx, id, next)
	})
	if err != nil {
		return AutomaticResult{}, fmt.Errorf("create automatic instance for job %d: %w", id, err)
	}
	if result.Created && s.hint != nil {
		s.hint(result.InstanceID)
	}
	if result.Reason == "misfire_ignored" || result.Reason == "blocked" || result.Reason == "duplicate" {
		reason := map[string]string{"misfire_ignored": "MISFIRE_IGNORED", "blocked": "BLOCK_DISCARDED", "duplicate": "AUTO_IDENTITY_EXISTS"}[result.Reason]
		logx.WithContext(ctx).Infow("job automatic trigger skipped", logx.Field("job_id", id),
			logx.Field("scheduled_at", scheduledAt), logx.Field("decision_at", decisionAt),
			logx.Field("next_trigger_at", nextTriggerAt), logx.Field("reason", reason))
	}
	return result, nil
}

func (s *Creator) acquire(ctx context.Context, wait bool) (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, s.budget)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, nil, err
	}
	if wait {
		// Automatic workers include quota waiting in their processing budget.
		// Manual requests instead reject immediately so callers can retry the UUID.
		select {
		case s.slots <- struct{}{}:
		case <-ctx.Done():
			cancel()
			return nil, nil, ctx.Err()
		}
	} else {
		select {
		case s.slots <- struct{}{}:
		default:
			cancel()
			return nil, nil, ErrCapacityBusy
		}
	}
	// Release only after Transaction returns, including a canceled operation
	// still unwinding in the driver. A detached goroutine would overbook slots.
	return ctx, func() { <-s.slots; cancel() }, nil
}

func availableGroup(ctx context.Context, r repository.Repositories, id int64) error {
	available, err := r.Groups.Available(ctx, id)
	if err != nil {
		return err
	}
	if !available {
		return repository.ErrExecutorGroupUnavailable
	}
	return nil
}

func replay(prior *repository.ManualIdentity, input ManualTrigger) (int64, error) {
	if prior.ParamsOverridden != input.ParamsOverridden || (input.ParamsOverridden && (prior.OverrideParams == nil || *prior.OverrideParams != input.Params)) {
		return 0, ErrRequestConflict
	}
	return prior.ID, nil
}

func snapshot(job *model.JobDefinition) *model.JobInstance {
	return &model.JobInstance{JobID: job.ID, JobName: job.Name, ExecutorGroupID: job.ExecutorGroupID, Status: model.InstanceWaiting,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ProcessorName: job.ProcessorName, ExecutionMode: job.ExecutionMode,
			PostEnabled: job.PostEnabled, Params: job.Params, MaxRetries: job.MaxRetries, RetryIntervalSeconds: job.RetryIntervalSeconds, TimeoutSeconds: job.TimeoutSeconds}}
}

func normalizeParams(value string) (string, error) {
	if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return "", ErrInvalidInput
	}
	return strings.TrimSpace(value), nil
}
