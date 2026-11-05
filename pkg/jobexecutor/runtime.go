package jobexecutor

import (
	"context"
	"encoding/json"
	"errors"
	"runtime/debug"
	"sync"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/core/logx"
)

// RuntimeConfig bounds one process's business work and result reporting. All
// processor stages share these limits. Zero selects the documented default;
// negative values are invalid. Concurrency includes workers waiting to hand off
// completed results. ReportCapacity includes waiting, backing-off and in-flight
// reports, independently of the business waiting queue.
type RuntimeConfig struct {
	// Concurrency defaults to 32 business workers, including blocked handoffs.
	Concurrency int `json:",default=32"`
	// QueueCapacity defaults to 128 waiting invocations, excluding workers.
	QueueCapacity int `json:",default=128"`
	// ReportCapacity defaults to 256 retained results, including in-flight calls.
	ReportCapacity int `json:",default=256"`
	// ReportConcurrency defaults to 16 simultaneous reporting calls.
	ReportConcurrency int `json:",default=16"`
}

func (c RuntimeConfig) defaults() (RuntimeConfig, error) {
	if c.Concurrency < 0 || c.QueueCapacity < 0 || c.ReportCapacity < 0 || c.ReportConcurrency < 0 {
		return c, errors.New("executor runtime limits must not be negative")
	}
	if c.Concurrency == 0 {
		c.Concurrency = 32
	}
	if c.QueueCapacity == 0 {
		c.QueueCapacity = 128
	}
	if c.ReportCapacity == 0 {
		c.ReportCapacity = 256
	}
	if c.ReportConcurrency == 0 {
		c.ReportConcurrency = 16
	}
	return c, nil
}

// Runtime owns bounded local admission, processor calls and result delivery.
// It implements Admission and is safe for concurrent use after construction.
// Each accepted attempt runs at most once while locally active; deduplication
// ends when the processor actually returns, not when reporting finishes. This
// is neither durable admission nor an exactly-once execution guarantee.
//
// Start must precede serving requests. Stop closes admission, Drain applies the
// host's shared shutdown budget, and Wait observes actual completion. Business
// code that ignores cancellation can keep Wait blocked after Drain times out.
// The host owns the RPC client and business dependencies and must not close them
// before actual completion. Runtime cannot be restarted or copied after use.
// Completion confirms workers have exited, not successful result delivery or
// database persistence; reporting can end after permanent refusal or exhaustion.
type Runtime struct {
	registry *Registry
	reporter *reporter
	config   RuntimeConfig
	ctx      context.Context
	cancel   context.CancelFunc
	queue    chan *activity
	done     chan struct{}

	mu        sync.Mutex
	active    map[attemptKey]*activity
	started   bool
	stopping  bool
	drainCtx  context.Context
	stopDrain func() bool
}

type attemptKey struct {
	taskID, attemptNo int64
}

// activity is protected by Runtime.mu, including the queued-to-running
// transition. Cancellation and dequeue therefore cannot lose each other's
// signals. A cancelled queued activity remains deduplicated until dequeued.
type activity struct {
	invocation Invocation
	cancelled  bool
	cancel     context.CancelFunc
}

var _ Admission = (*Runtime)(nil)

// NewRuntime validates configuration and retains the supplied dependencies.
// It starts no goroutines and owns no RPC connection. Credentials and the
// canonical UUIDv4 process identity must match the executor RPC adapter. The
// registry remains mutable until Start or NewRPCAdapter freezes it.
func NewRuntime(registry *Registry, client protocol.ResultReceiverClient, groupName, token, executorInstanceID string, config RuntimeConfig) (*Runtime, error) {
	if registry == nil {
		return nil, errors.New("executor registry must not be nil")
	}
	config, err := config.defaults()
	if err != nil {
		return nil, err
	}
	auth, err := NewAuthenticator(groupName, token)
	if err != nil {
		return nil, err
	}
	reports, err := newReporter(client, auth, executorInstanceID, config.ReportCapacity, config.ReportConcurrency)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Runtime{
		registry: registry, reporter: reports, config: config, ctx: ctx, cancel: cancel,
		queue: make(chan *activity, config.QueueCapacity), done: make(chan struct{}),
		active: make(map[attemptKey]*activity),
	}, nil
}

// Start freezes registration and starts fixed business and reporting workers.
// Readiness is published before returning nil. Repeated Start is an error;
// Start after Stop returns ErrStopping, including when Stop preceded Start.
func (r *Runtime) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return ErrStopping
	}
	if r.started {
		return errors.New("executor runtime has already started")
	}
	if err := r.reporter.start(r.ctx); err != nil {
		return err
	}
	r.registry.Freeze()
	r.started = true
	var workers sync.WaitGroup
	for range r.config.Concurrency {
		workers.Go(r.work)
	}
	go func() {
		workers.Wait()
		// A processor can still be handing off its last result after Stop. Close
		// reporting admission only after all such producers have actually exited.
		r.reporter.stop()
		_ = r.reporter.wait(context.Background())
		r.mu.Lock()
		defer r.mu.Unlock()
		r.cancel()
		if r.stopDrain != nil {
			r.stopDrain()
		}
		close(r.done)
	}()
	return nil
}

// Accept atomically deduplicates and admits validated work without waiting for
// queue space. Active duplicates win over stopping, expiry and a cancelled RPC.
// False with nil error means definitely full; no work was admitted. The caller
// must not mutate Invocation.Items concurrently with this call; accepted items
// are copied. Business execution never inherits ctx or its deadline.
func (r *Runtime) Accept(ctx context.Context, invocation Invocation) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := attemptKey{invocation.TaskID, invocation.AttemptNo}
	if _, exists := r.active[key]; exists {
		return true, nil
	}
	if r.stopping {
		return false, ErrStopping
	}
	if !r.started {
		return false, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !time.Now().Before(invocation.DeadlineAt) {
		return false, ErrExpired
	}
	if err := r.validateInvocation(invocation); err != nil {
		return false, err
	}
	if invocation.Items != nil {
		items := make([]json.RawMessage, len(invocation.Items))
		for index, item := range invocation.Items {
			items[index] = append(json.RawMessage(nil), item...)
		}
		invocation.Items = items
	}
	work := &activity{invocation: invocation}
	select {
	case r.queue <- work:
		// Dequeue also acquires mu, so no worker can finish before this insert.
		r.active[key] = work
		return true, nil
	default:
		return false, nil
	}
}

// Cancel signals current activities for an Instance without waiting for their
// exit. Queued work is skipped and need not report a result. Running work keeps
// its slot and identity until it actually returns. No tombstone is retained:
// later arrivals may be admitted. Repeated cancellation is harmless.
func (r *Runtime) Cancel(ctx context.Context, instanceID int64, reason CancelReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if instanceID <= 0 || (reason != CancelManualStop && reason != CancelInstanceTimeout) {
		return errors.New("executor cancellation is invalid")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, work := range r.active {
		if work.invocation.InstanceID == instanceID {
			work.cancelled = true
			if work.cancel != nil {
				work.cancel()
			}
		}
	}
	return nil
}

// Stop promptly closes new business admission. Accepted work and reporting
// continue until drained or the shared Drain budget expires. Stop is idempotent
// and safe before Start; it neither closes client connections nor waits.
func (r *Runtime) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return
	}
	r.stopping = true
	close(r.queue)
	if !r.started {
		r.cancel()
		close(r.done)
	}
}

// Drain calls Stop and waits using the host's shared shutdown budget. The first
// call fixes that budget; later calls cannot extend it. Budget cancellation
// stops queued starts, signals running business and aborts result delivery, but
// does not prove that business has exited. A later caller can bound its own wait
// more tightly. Use Wait to observe actual completion after a timeout.
func (r *Runtime) Drain(ctx context.Context) error {
	r.Stop()
	r.mu.Lock()
	if r.drainCtx == nil {
		r.drainCtx = ctx
		r.stopDrain = context.AfterFunc(ctx, r.cancel)
	}
	budget := r.drainCtx
	r.mu.Unlock()
	select {
	case <-r.done:
		r.mu.Lock()
		r.stopDrain()
		r.mu.Unlock()
		return nil
	default:
	}
	select {
	case <-r.done:
		return nil
	case <-budget.Done():
		r.cancel()
		return budget.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wait observes actual business and reporting completion, without stopping the
// runtime or changing its shutdown budget. It returns ErrNotReady before Start
// unless Stop has already closed the unused runtime. Cancellation bounds only
// this wait and never authorizes closing dependencies still used by business.
func (r *Runtime) Wait(ctx context.Context) error {
	r.mu.Lock()
	ready := r.started || r.stopping
	r.mu.Unlock()
	if !ready {
		return ErrNotReady
	}
	select {
	case <-r.done:
		return nil
	default:
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) work() {
	for work := range r.queue {
		r.execute(work)
	}
}

func (r *Runtime) execute(work *activity) {
	in := work.invocation
	key := attemptKey{in.TaskID, in.AttemptNo}
	r.mu.Lock()
	if work.cancelled || r.ctx.Err() != nil || !time.Now().Before(in.DeadlineAt) {
		delete(r.active, key)
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithDeadline(r.ctx, in.DeadlineAt)
	work.cancel = cancel
	r.mu.Unlock()
	result, stack := r.invoke(ctx, in)
	cancel()
	r.mu.Lock()
	delete(r.active, key)
	r.mu.Unlock()
	// Activity ends at actual business return, before diagnostics or a potentially
	// blocked handoff. The worker remains occupied until the handoff completes.
	if stack != nil {
		logx.Errorw("executor processor panicked", logx.Field("task_id", in.TaskID),
			logx.Field("attempt_no", in.AttemptNo), logx.Field("stack", string(stack)))
	}
	// The business context may already be expired or cancelled. Results use the
	// runtime lifecycle so cancellation cannot erase a completed business outcome.
	if err := r.reporter.submit(r.ctx, result); err != nil {
		reportDiscards.Inc("shutdown")
		logx.Errorw("executor result handoff stopped", logx.Field("task_id", in.TaskID),
			logx.Field("attempt_no", in.AttemptNo))
	}
}

// invoke recovers only panics on the processor's calling goroutine. It never
// logs the panic value or business payload, which may contain credentials.
func (r *Runtime) invoke(ctx context.Context, in Invocation) (report executionResult, stack []byte) {
	report = executionResult{jobID: in.JobID, instanceID: in.InstanceID, taskID: in.TaskID, attemptNo: in.AttemptNo}
	defer func() {
		if recover() != nil {
			report.success = false
			report.summary = "processor panicked"
			report.data, report.preparedItems = nil, nil
			stack = debug.Stack()
		}
	}()
	var result Result
	var err error
	switch in.Stage {
	case StageOrdinary:
		processor, _ := r.registry.Lookup(in.ProcessorName)
		result, err = processor.Execute(ctx, in.Input)
	case StagePrepare:
		processor, _ := r.registry.LookupSharding(in.ProcessorName)
		var prepared PrepareResult
		prepared, err = processor.PreProcess(ctx, in.Input)
		report.prepared, report.preparedItems = true, prepared.Items
		result.Summary = prepared.Summary
	case StageShard:
		processor, _ := r.registry.LookupSharding(in.ProcessorName)
		result, err = processor.ShardingProcess(ctx, ShardInput{Input: in.Input, Items: in.Items})
	case StagePost:
		processor, _ := r.registry.LookupSharding(in.ProcessorName)
		result, err = processor.(PostProcessor).PostProcess(ctx, PostInput{
			Input: in.Input, Total: in.Total, Succeeded: in.Succeeded, Failed: in.Failed,
		})
	}
	if err != nil {
		report.summary = err.Error()
		report.preparedItems = nil
		return report, nil
	}
	report.success, report.summary, report.data = true, result.Summary, result.Data
	return report, nil
}

// validateInvocation protects direct Admission callers. Wire authentication and
// full request validation remain at RPCAdapter; this runtime never returns RPC
// codes. Registration is frozen, so capabilities cannot change after admission.
func (r *Runtime) validateInvocation(in Invocation) error {
	if in.JobID <= 0 || in.InstanceID <= 0 || in.TaskID <= 0 || in.AttemptNo <= 0 {
		return errors.New("execution IDs and attempt number must be positive")
	}
	if in.Stage == StageOrdinary && !in.RequiredPost {
		if _, ok := r.registry.Lookup(in.ProcessorName); ok {
			return nil
		}
	} else if in.Stage == StagePrepare || in.Stage == StageShard || (in.Stage == StagePost && in.RequiredPost) {
		if processor, ok := r.registry.LookupSharding(in.ProcessorName); ok {
			if !in.RequiredPost {
				return nil
			}
			if _, ok := processor.(PostProcessor); ok {
				return nil
			}
		}
	}
	return errors.New("execution stage or processor capability is invalid")
}
