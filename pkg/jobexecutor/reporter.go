package jobexecutor

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/rand/v2"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	reportAttemptLimit = 7
	reportCallTimeout  = 2 * time.Second
	// reportMessageLimit matches gRPC's existing client send default, not the
	// server's default receive limit or a separate business result restriction.
	reportMessageLimit = math.MaxInt32
)

var reportDiscards = metric.NewCounterVec(&metric.CounterVecOpts{
	Namespace: "dogx", Subsystem: "jobexecutor", Name: "report_discarded_total",
	Help: "Executor results discarded before scheduler admission acknowledgement.", Labels: []string{"reason"},
})

// executionResult is a completed processor outcome without transport credentials.
// The producer owns its JSON buffers until submit succeeds; it must not modify
// them afterwards. Failed outcomes ignore both ordinary and preparation output.
type executionResult struct {
	jobID, instanceID, taskID, attemptNo int64
	success                              bool
	summary                              string
	data                                 json.RawMessage
	prepared                             bool
	preparedItems                        []json.RawMessage
}

// reporter owns bounded, volatile result retention, including queued retries and
// in-flight calls. Retrying releases the worker but not the retention position.
// It neither changes business outcomes nor assumes an ACK means a database commit.
// All methods support concurrent calls; start is single-use. The Runtime owns the
// supplied lifecycle Context and client connection, and must wait for actual worker
// completion before closing that connection. There is no reporter-owned grace period.
type reporter struct {
	client      protocol.ResultReceiverClient
	auth        Authenticator
	executorID  string
	capacity    int
	concurrency int

	mu        sync.Mutex
	started   bool
	stopping  bool
	completed bool
	runCtx    context.Context
	retained  int
	sequence  uint64
	pending   reportHeap
	changed   chan struct{}
	done      chan struct{}
}

type retainedReport struct {
	result   executionResult
	due      time.Time
	sequence uint64
	attempts int
}

// newReporter validates immutable communication dependencies without starting
// workers. Capacity and concurrency must be positive; the Runtime selects defaults.
func newReporter(client protocol.ResultReceiverClient, auth *Authenticator, executorID string, capacity, concurrency int) (*reporter, error) {
	if nilReportClient(client) {
		return nil, errors.New("result reporting client must not be nil")
	}
	if auth == nil {
		return nil, errors.New("result reporting authenticator must not be nil")
	}
	if err := ValidateCredentials(auth.groupName, auth.token); err != nil {
		return nil, err
	}
	if !validExecutorInstanceID(executorID) {
		return nil, errors.New("result reporting executor identity is invalid")
	}
	if capacity <= 0 || concurrency <= 0 {
		return nil, errors.New("result reporting capacity and concurrency must be positive")
	}
	return &reporter{client: client, auth: *auth, executorID: executorID,
		capacity: capacity, concurrency: concurrency, changed: make(chan struct{}), done: make(chan struct{})}, nil
}

// start synchronously opens submission and starts fixed workers under the whole
// Runtime lifetime, not a business execution or request Context. Cancellation
// refuses new submissions, interrupts active calls and prevents further retries.
// A canceled or nil Context is rejected without starting workers.
func (r *reporter) start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("result reporting context must not be nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopping {
		return errors.New("result reporter has already started or stopped")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.runCtx, r.started = ctx, true
	var workers sync.WaitGroup
	for range r.concurrency {
		workers.Go(r.work)
	}
	go func() {
		workers.Wait()
		r.finish()
	}()
	return nil
}

// submit transfers immutable result ownership on nil error, waiting for capacity
// when necessary. Only the finite business workers may block here. This Context
// bounds handoff; its later cancellation never withdraws an accepted result.
// ErrNotReady or ErrStopping leaves ownership with the producer, as does any
// other error. stop wakes blocked submissions and refuses them without admission.
func (r *reporter) submit(ctx context.Context, result executionResult) error {
	if ctx == nil {
		return errors.New("result submission context must not be nil")
	}
	for {
		r.mu.Lock()
		if err := ctx.Err(); err != nil {
			r.mu.Unlock()
			return err
		}
		if !r.started {
			stopping := r.stopping
			r.mu.Unlock()
			if stopping {
				return ErrStopping
			}
			return ErrNotReady
		}
		if r.stopping || r.runCtx.Err() != nil {
			r.mu.Unlock()
			return ErrStopping
		}
		if r.retained < r.capacity {
			r.sequence++
			heap.Push(&r.pending, &retainedReport{result: result, due: time.Now(), sequence: r.sequence})
			r.retained++
			r.notifyLocked()
			r.mu.Unlock()
			return nil
		}
		changed, runDone := r.changed, r.runCtx.Done()
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-runDone:
			return ErrStopping
		case <-changed:
		}
	}
}

// stop closes new handoffs and drains all accepted results without canceling them.
// The Runtime calls it after all business workers have exited. The lifecycle
// Context still bounds outstanding calls and retries; repeated calls are safe.
func (r *reporter) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopping = true
	r.notifyLocked()
	if !r.started && !r.completed {
		r.completed = true
		close(r.done)
	}
}

// wait observes actual worker completion, not merely cancellation. It does not
// stop reporting or grant another drain budget. Nil confirms that client use has
// ended, not that all retained results reached the scheduler. Callers may retry a
// timed-out wait; already completed work takes precedence over an expired Context.
func (r *reporter) wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("result reporting wait context must not be nil")
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

func (r *reporter) work() {
	for {
		entry := r.take()
		if entry == nil {
			return
		}
		request, discard := r.request(entry.result)
		acknowledged := false
		if discard == "" && r.runCtx.Err() == nil {
			ctx, cancel := context.WithTimeout(r.runCtx, reportCallTimeout)
			entry.attempts++
			// Keep the preflight size check aligned with the actual client limit.
			// Unknown lower remote limits remain retryable ResourceExhausted errors.
			response, err := r.client.ReportTaskResult(ctx, request, grpc.MaxCallSendMsgSize(reportMessageLimit))
			cancel()
			acknowledged = err == nil && response != nil
			if !acknowledged {
				discard = permanentReportFailure(err)
				if discard == "" && entry.attempts == reportAttemptLimit {
					discard = "retry_exhausted"
				}
			}
		}
		finishedAt := time.Now()
		r.mu.Lock()
		if acknowledged || discard != "" {
			r.retained--
		} else if r.runCtx.Err() == nil {
			entry.due = finishedAt.Add(reportRetryDelay(entry.attempts))
			heap.Push(&r.pending, entry)
		}
		r.notifyLocked()
		r.mu.Unlock()
		if discard != "" {
			reportDiscards.Inc(discard)
			// Transport errors, processor output and credentials can contain secrets.
			// Only fixed classifications and execution identities belong in logs.
			logx.Errorw("job executor result discarded",
				logx.Field("job_id", entry.result.jobID), logx.Field("instance_id", entry.result.instanceID),
				logx.Field("task_id", entry.result.taskID), logx.Field("attempt_no", entry.result.attemptNo),
				logx.Field("report_attempts", entry.attempts), logx.Field("reason", discard))
		}
	}
}

func (r *reporter) take() *retainedReport {
	for {
		r.mu.Lock()
		if r.runCtx.Err() != nil || (r.stopping && r.retained == 0) {
			r.mu.Unlock()
			return nil
		}
		changed := r.changed
		var delay time.Duration
		if len(r.pending) != 0 {
			delay = time.Until(r.pending[0].due)
			if delay <= 0 {
				entry := heap.Pop(&r.pending).(*retainedReport)
				r.mu.Unlock()
				return entry
			}
		}
		r.mu.Unlock()
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-changed:
			case <-r.runCtx.Done():
			}
			timer.Stop()
		} else {
			select {
			case <-changed:
			case <-r.runCtx.Done():
			}
		}
	}
}

func (r *reporter) finish() {
	r.mu.Lock()
	unsent := r.retained
	r.pending, r.retained = nil, 0
	r.mu.Unlock()
	if unsent != 0 {
		reportDiscards.Add(float64(unsent), "shutdown")
		logx.Errorw("job executor reporting stopped with unacknowledged results", logx.Field("discarded_results", unsent))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed = true
	close(r.done)
}

// request adds machine identity only at the transport boundary. Validation rejects
// malformed successful output without fabricating business failure. An explicit
// empty preparation output still selects its oneof branch; nil ordinary Data does not.
func (r *reporter) request(result executionResult) (*protocol.ReportTaskResultRequest, string) {
	request := &protocol.ReportTaskResultRequest{
		Auth:  &protocol.GroupAuth{GroupName: r.auth.groupName, Token: r.auth.token},
		JobId: result.jobID, InstanceId: result.instanceID, TaskId: result.taskID,
		AttemptNo: result.attemptNo, ExecutorInstanceId: r.executorID,
		Outcome: protocol.TaskOutcome_FAILURE, Summary: result.summary,
	}
	if !result.success {
		return request, reportMessageProblem(request, reportMessageLimit)
	}
	request.Outcome = protocol.TaskOutcome_SUCCESS
	if result.prepared {
		if err := (PrepareResult{Items: result.preparedItems}).Validate(); err != nil {
			return nil, "invalid_output"
		}
		items := make([][]byte, len(result.preparedItems))
		for i, item := range result.preparedItems {
			items[i] = item
		}
		request.Output = &protocol.ReportTaskResultRequest_PreparedItems{PreparedItems: &protocol.PreparedItems{Items: items}}
	} else {
		if err := (Result{Data: result.data}).Validate(); err != nil {
			return nil, "invalid_output"
		}
		if result.data != nil {
			request.Output = &protocol.ReportTaskResultRequest_DataJson{DataJson: result.data}
		}
	}
	return request, reportMessageProblem(request, reportMessageLimit)
}

// reportMessageProblem detects definite local encoding failures without making a
// second full encoded copy. These are the request's only protobuf string fields;
// JSON outputs are bytes and are validated separately. The limit must match the
// actual client send option. A remote receiver can have an unknown lower limit.
func reportMessageProblem(request *protocol.ReportTaskResultRequest, limit int) string {
	if request == nil || !utf8.ValidString(request.GetSummary()) || !utf8.ValidString(request.GetExecutorInstanceId()) ||
		!utf8.ValidString(request.GetAuth().GetGroupName()) || !utf8.ValidString(request.GetAuth().GetToken()) {
		return "invalid_encoding"
	}
	if proto.Size(request) > limit {
		return "message_too_large"
	}
	return ""
}

func permanentReportFailure(err error) string {
	if err == nil {
		return ""
	}
	st := status.Convert(err)
	switch st.Code() {
	case codes.Unauthenticated:
		return "unauthenticated"
	case codes.PermissionDenied:
		return "permission_denied"
	}
	if reason, ok := bizerror.SubcodeFromStatus(st); ok && reason == subcode.InvalidRequest {
		return "invalid_request"
	}
	return ""
}

func reportRetryDelay(attempts int) time.Duration {
	steps := [...]time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 16 * time.Second}
	index := min(max(attempts-1, 0), len(steps)-1)
	return time.Duration(float64(steps[index]) * (0.8 + rand.Float64()*0.4))
}

func nilReportClient(client protocol.ResultReceiverClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (r *reporter) notifyLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

// reportHeap gives already-due retries priority over newer submissions. Sequence
// preserves original admission order when due times match; in-flight entries are
// absent, so no two workers can report the same retained result concurrently.
type reportHeap []*retainedReport

func (h reportHeap) Len() int { return len(h) }
func (h reportHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].sequence < h[j].sequence
	}
	return h[i].due.Before(h[j].due)
}
func (h reportHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *reportHeap) Push(value any) { *h = append(*h, value.(*retainedReport)) }
func (h *reportHeap) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return entry
}
