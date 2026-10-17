package service

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
	"gorm.io/gorm"
)

const (
	DefaultCreationCapacity = 32
	DefaultCreationBudget   = 3 * time.Second
	MisfireTolerance        = 5 * time.Second
)

type repositories struct {
	jobs      repository.JobDefinitionRepository
	groups    repository.ExecutorGroupRepository
	instances repository.JobInstanceRepository
}

type Service struct {
	repos    repositories
	transact func(context.Context, func(repositories) error) error
	now      func() time.Time
	notify   func(context.Context) error
	hint     func(int64)
	capacity int
	budget   time.Duration
	slots    chan struct{}
}

type Option func(*Service)

func WithClock(now func() time.Time) Option { return func(s *Service) { s.now = now } }
func WithNotifier(notify func(context.Context) error) Option {
	return func(s *Service) { s.notify = notify }
}

// The hint consumer must be nonblocking; persistence, not this hint, is the handoff.
func WithInstanceHint(hint func(int64)) Option { return func(s *Service) { s.hint = hint } }
func WithCreationLimits(capacity int, budget time.Duration) Option {
	return func(s *Service) { s.capacity, s.budget = capacity, budget }
}

func New(db *gorm.DB, options ...Option) (*Service, error) {
	if db == nil {
		return nil, errors.New("job service database is nil")
	}
	s := &Service{now: time.Now, capacity: DefaultCreationCapacity, budget: DefaultCreationBudget}
	for _, option := range options {
		option(s)
	}
	if s.now == nil || s.capacity <= 0 || s.budget <= 0 {
		return nil, errors.New("invalid job service clock or creation limits")
	}
	s.slots = make(chan struct{}, s.capacity)
	s.repos = bind(db)
	s.transact = func(ctx context.Context, fn func(repositories) error) error {
		// GORM's default transaction retains PostgreSQL READ COMMITTED. Every
		// repository below uses the same handle; external notification is post-commit.
		return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return fn(bind(tx)) })
	}
	return s, nil
}

func bind(db *gorm.DB) repositories {
	return repositories{repository.NewJobDefinitionRepository(db), repository.NewExecutorGroupRepository(db), repository.NewJobInstanceRepository(db)}
}

func (s *Service) ListJobs(ctx context.Context, filter repository.JobFilter) ([]repository.JobListItem, int64, error) {
	if filter.Page < 1 || filter.PageSize < 1 || filter.PageSize > 200 || filter.Page-1 > (1<<63-1)/filter.PageSize ||
		(filter.ID != nil && *filter.ID <= 0) || (filter.ExecutorGroupID != nil && *filter.ExecutorGroupID <= 0) ||
		(filter.Status != nil && *filter.Status != basemodel.RecordStatusDisabled && *filter.Status != basemodel.RecordStatusEnabled) {
		return nil, 0, ErrInvalidInput
	}
	var err error
	filter.Name, err = normalizeString(filter.Name, 128, false)
	if err != nil {
		return nil, 0, err
	}
	return s.repos.jobs.List(ctx, filter)
}

func (s *Service) GetJob(ctx context.Context, id int64) (*repository.JobDetail, error) {
	if id <= 0 {
		return nil, ErrInvalidInput
	}
	return s.repos.jobs.Get(ctx, id)
}

func (s *Service) CreateJob(ctx context.Context, input *model.JobDefinition) (int64, error) {
	job, err := normalizedDefinition(input)
	if err != nil {
		return 0, err
	}
	if job.Status != basemodel.RecordStatusDisabled && job.Status != basemodel.RecordStatusEnabled {
		return 0, ErrInvalidInput
	}
	// Validate even a disabled definition before acquiring database resources.
	if _, err := schedule.Next(job.CronExpr, s.now()); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	var id int64
	err = s.transact(ctx, func(r repositories) error {
		if err := r.groups.LockAvailable(ctx, job.ExecutorGroupID); err != nil {
			return err
		}
		job.NextTriggerAt = nil
		if job.Status == basemodel.RecordStatusEnabled {
			next, err := schedule.Next(job.CronExpr, s.now())
			if err != nil {
				return fmt.Errorf("%w: %w", ErrInvalidInput, err)
			}
			job.NextTriggerAt = &next
		}
		var err error
		id, err = r.jobs.Create(ctx, job)
		return err
	})
	if err != nil {
		return 0, err
	}
	s.changed(ctx)
	return id, nil
}

func (s *Service) UpdateJob(ctx context.Context, id int64, input *model.JobDefinition) error {
	if id <= 0 {
		return ErrInvalidInput
	}
	job, err := normalizedDefinition(input)
	if err != nil {
		return err
	}
	if _, err := schedule.Next(job.CronExpr, s.now()); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	err = s.transact(ctx, func(r repositories) error {
		if err := r.groups.LockAvailable(ctx, job.ExecutorGroupID); err != nil {
			return err
		}
		return r.jobs.Update(ctx, id, job)
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

func (s *Service) UpdateJobStatus(ctx context.Context, id int64, status basemodel.RecordStatus) error {
	if id <= 0 || (status != basemodel.RecordStatusDisabled && status != basemodel.RecordStatusEnabled) {
		return ErrInvalidInput
	}
	var err error
	if status == basemodel.RecordStatusDisabled {
		err = s.repos.jobs.Disable(ctx, id)
	} else {
		err = s.transact(ctx, func(r repositories) error {
			job, err := r.jobs.Lock(ctx, id)
			if err != nil {
				return err
			}
			if job.Status == basemodel.RecordStatusEnabled {
				return nil
			}
			if err := availableGroup(ctx, r, job.ExecutorGroupID); err != nil {
				return err
			}
			if _, err := normalizedDefinition(job); err != nil {
				return err
			}
			next, err := schedule.Next(job.CronExpr, s.now())
			if err != nil {
				return fmt.Errorf("%w: %w", ErrInvalidInput, err)
			}
			return r.jobs.Enable(ctx, id, next)
		})
	}
	if err == nil {
		s.changed(ctx)
	}
	return err
}

func (s *Service) DeleteJob(ctx context.Context, id int64) error {
	if id <= 0 {
		return ErrInvalidInput
	}
	err := s.transact(ctx, func(r repositories) error {
		job, err := r.jobs.Lock(ctx, id)
		if err != nil {
			return err
		}
		if job.Status != basemodel.RecordStatusDisabled {
			return ErrStateNotAllowed
		}
		active, err := r.instances.HasActive(ctx, id)
		if err != nil {
			return err
		}
		if active {
			return ErrActiveInstances
		}
		return r.jobs.Delete(ctx, id)
	})
	if err == nil {
		s.changed(ctx)
	}
	return err
}

type ManualTrigger struct {
	JobID            int64
	RequestID        uuid.UUID
	ParamsOverridden bool
	Params           string
	OperatorID       int64
}

func (s *Service) TriggerManual(ctx context.Context, input ManualTrigger) (int64, error) {
	if input.JobID <= 0 || input.OperatorID <= 0 || (!input.ParamsOverridden && input.Params != "") {
		return 0, ErrInvalidInput
	}
	var err error
	input.Params, err = normalizeString(input.Params, 0, false)
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
	err = s.transact(ctx, func(r repositories) error {
		job, err := r.jobs.Lock(ctx, input.JobID)
		if err != nil {
			return err
		}
		prior, err := r.instances.FindManual(ctx, input.JobID, input.RequestID)
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
			active, err := r.instances.HasActive(ctx, job.ID)
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
		id, created, err = r.instances.Insert(ctx, instance)
		if err != nil || created {
			return err
		}
		// A conflict's tuple can become visible only to a new READ COMMITTED
		// statement. Never combine INSERT and fallback SELECT into one CTE.
		prior, err = r.instances.FindManual(ctx, input.JobID, input.RequestID)
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
		return 0, err
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

func (s *Service) TriggerAutomatic(ctx context.Context, id int64) (AutomaticResult, error) {
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
	err = s.transact(ctx, func(r repositories) error {
		job, err := r.jobs.Lock(ctx, id)
		if errors.Is(err, ErrNotFound) {
			result.Reason = "not_found"
			return nil
		}
		if err != nil {
			return err
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
			active, err := r.instances.HasActive(ctx, id)
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
			result.InstanceID, result.Created, err = r.instances.Insert(ctx, instance)
			if err != nil {
				return err
			}
			if result.Created {
				result.Reason = "created"
			} else {
				result.Reason = "duplicate"
			}
		}
		return r.jobs.Advance(ctx, id, next)
	})
	if err != nil {
		return AutomaticResult{}, err
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

func (s *Service) LoadRules(ctx context.Context) ([]repository.Rule, error) {
	return s.repos.jobs.LoadRules(ctx)
}
func (s *Service) ListDue(ctx context.Context, at time.Time, limit int) ([]repository.DueJob, error) {
	return s.repos.jobs.ListDue(ctx, at, limit)
}

func (s *Service) acquire(ctx context.Context, wait bool) (context.Context, func(), error) {
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

func (s *Service) changed(ctx context.Context) {
	if s.notify == nil || ctx.Err() != nil {
		return
	}
	// Notification shares a shorter upstream deadline. A timeout or cancellation
	// never reverses the confirmed commit; minute reconciliation fills any gap.
	notifyCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := s.notify(notifyCtx); err != nil {
		logx.WithContext(notifyCtx).Errorw("job configuration notification failed", logx.Field("error", err))
	}
}

func availableGroup(ctx context.Context, r repositories, id int64) error {
	available, err := r.groups.Available(ctx, id)
	if err != nil {
		return err
	}
	if !available {
		return ErrExecutorGroupUnavailable
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

func normalizedDefinition(input *model.JobDefinition) (*model.JobDefinition, error) {
	if input == nil {
		return nil, ErrInvalidInput
	}
	job := *input
	fields := []struct {
		value    *string
		limit    int
		required bool
	}{
		{&job.Name, 128, true}, {&job.Remark, 500, false}, {&job.ProcessorName, 128, true}, {&job.Params, 0, false}, {&job.CronExpr, 128, true},
	}
	for _, field := range fields {
		value, err := normalizeString(*field.value, field.limit, field.required)
		if err != nil {
			return nil, err
		}
		*field.value = value
	}
	if job.ExecutorGroupID <= 0 || (job.ExecutionMode != model.ExecutionModeNormal && job.ExecutionMode != model.ExecutionModeSharded) ||
		(job.ExecutionMode == model.ExecutionModeNormal && job.PostEnabled) ||
		(job.MisfirePolicy != model.MisfireIgnore && job.MisfirePolicy != model.MisfireOnce) ||
		(job.BlockPolicy != model.BlockDiscard && job.BlockPolicy != model.BlockParallel) ||
		job.MaxRetries < 0 || job.RetryIntervalSeconds <= 0 || job.TimeoutSeconds <= 0 {
		return nil, ErrInvalidInput
	}
	// int32 seconds fit time.Duration; retain explicit checks if the transport type grows.
	if int64(job.TimeoutSeconds) > int64((1<<63-1)/time.Second) || int64(job.RetryIntervalSeconds) > int64((1<<63-1)/time.Second) {
		return nil, ErrInvalidInput
	}
	return &job, nil
}

func normalizeString(value string, limit int, required bool) (string, error) {
	if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return "", ErrInvalidInput
	}
	value = strings.TrimSpace(value)
	if (required && value == "") || (limit > 0 && utf8.RuneCountInString(value) > limit) {
		return "", ErrInvalidInput
	}
	return value, nil
}
