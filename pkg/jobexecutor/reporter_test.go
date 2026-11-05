package jobexecutor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestReporterValidatesDependenciesAndLifecycle(t *testing.T) {
	auth, err := NewAuthenticator(adapterTestGroup, adapterTestToken)
	if err != nil {
		t.Fatal(err)
	}
	var typedNil *reportClientStub
	var nilFunction reportClientFunction
	for _, client := range []protocol.ResultReceiverClient{nil, typedNil, nilFunction} {
		if r, err := newReporter(client, auth, adapterTestID, 1, 1); r != nil || err == nil {
			t.Fatal("nil reporting client accepted")
		}
	}
	for _, invalid := range []*Authenticator{nil, {}, {groupName: adapterTestGroup, token: "invalid"}} {
		if r, err := newReporter(&reportClientStub{}, invalid, adapterTestID, 1, 1); r != nil || err == nil {
			t.Fatal("invalid reporting credentials accepted")
		}
	}
	if r, err := newReporter(&reportClientStub{}, auth, "invalid", 1, 1); r != nil || err == nil {
		t.Fatal("invalid executor identity accepted")
	}
	for _, settings := range [][2]int{{0, 1}, {1, 0}, {-1, 1}, {1, -1}} {
		if r, err := newReporter(&reportClientStub{}, auth, adapterTestID, settings[0], settings[1]); r != nil || err == nil {
			t.Fatal("invalid reporting limits accepted")
		}
	}
	r, err := newReporter(&reportClientStub{}, auth, adapterTestID, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.submit(context.Background(), reporterResult(1)); !errors.Is(err, ErrNotReady) {
		t.Fatalf("unstarted submission: %v", err)
	}
	if r.start(nil) == nil || r.submit(nil, reporterResult(1)) == nil || r.wait(nil) == nil {
		t.Fatal("nil lifecycle context accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.start(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup: %v", err)
	}
	r.stop()
	r.stop()
	if err := r.start(context.Background()); err == nil {
		t.Fatal("stopped reporter restarted")
	}
	if err := r.submit(context.Background(), reporterResult(1)); !errors.Is(err, ErrStopping) {
		t.Fatalf("stopped submission: %v", err)
	}
	if err := r.wait(canceled); err != nil {
		t.Fatalf("completed wait lost to expired context: %v", err)
	}
}

func TestReporterCapacityBlocksHandoffWithoutOwningItsLaterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		defer releaseOnce()
		client := &reportClientStub{call: func(ctx context.Context, request *protocol.ReportTaskResultRequest, _ int) (*protocol.ReportTaskResultResponse, error) {
			if request.TaskId == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &protocol.ReportTaskResultResponse{}, nil
		}}
		r := startTestReporter(t, client, context.Background(), 2, 1)
		if err := r.start(context.Background()); err == nil {
			t.Fatal("reporter started duplicate workers")
		}
		handoff, cancelHandoff := context.WithCancel(context.Background())
		if err := r.submit(handoff, reporterResult(1)); err != nil {
			t.Fatal(err)
		}
		cancelHandoff()
		synctest.Wait()
		calls := client.snapshot()
		if len(calls) != 1 || calls[0].ctx.Err() != nil {
			t.Fatal("accepted report inherited its submission context cancellation")
		}
		if err := r.submit(context.Background(), reporterResult(2)); err != nil {
			t.Fatal(err)
		}
		waiting, cancelWaiting := context.WithCancel(context.Background())
		handedOff := make(chan error, 1)
		go func() { handedOff <- r.submit(waiting, reporterResult(3)) }()
		synctest.Wait()
		select {
		case err := <-handedOff:
			t.Fatalf("full buffer did not block finite business worker: %v", err)
		default:
		}
		cancelWaiting()
		if err := <-handedOff; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled pending handoff: %v", err)
		}
		go func() { handedOff <- r.submit(context.Background(), reporterResult(4)) }()
		synctest.Wait()
		r.stop()
		if err := <-handedOff; !errors.Is(err, ErrStopping) {
			t.Fatalf("stop did not wake a blocked submission: %v", err)
		}
		if calls[0].ctx.Err() != nil {
			t.Fatal("stop canceled accepted reporting work")
		}
		if err := r.submit(context.Background(), reporterResult(4)); !errors.Is(err, ErrStopping) {
			t.Fatalf("late submission: %v", err)
		}
		releaseOnce()
		if err := r.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := reportTaskIDs(client.snapshot()); !reflect.DeepEqual(got, []int64{1, 2}) {
			t.Fatalf("drained reports = %v", got)
		}
	})
}

func TestReporterLifecycleCancellationBoundsConcurrencyAndWakesSubmitters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := &reportClientStub{call: func(ctx context.Context, _ *protocol.ReportTaskResultRequest, _ int) (*protocol.ReportTaskResultResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		r := startTestReporter(t, client, ctx, 4, 2)
		for id := int64(1); id <= 4; id++ {
			if err := r.submit(context.Background(), reporterResult(id)); err != nil {
				t.Fatal(err)
			}
		}
		pending := make(chan error, 1)
		go func() { pending <- r.submit(context.Background(), reporterResult(5)) }()
		synctest.Wait()
		if len(client.snapshot()) != 2 {
			t.Fatal("reporting concurrency exceeded its fixed worker count")
		}
		select {
		case err := <-pending:
			t.Fatalf("capacity omitted queued or in-flight reports: %v", err)
		default:
		}
		cancel()
		if err := <-pending; !errors.Is(err, ErrStopping) {
			t.Fatalf("runtime cancellation did not refuse blocked handoff: %v", err)
		}
		if err := r.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(client.snapshot()) != 2 {
			t.Fatal("lifecycle cancellation sent a pending result or another retry")
		}
		if err := r.submit(context.Background(), reporterResult(6)); !errors.Is(err, ErrStopping) {
			t.Fatalf("late submission after cancellation: %v", err)
		}
	})
}

func TestReporterSevenActualCallsUseCompletionBasedBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &reportClientStub{call: func(context.Context, *protocol.ReportTaskResultRequest, int) (*protocol.ReportTaskResultResponse, error) {
			time.Sleep(100 * time.Millisecond)
			return nil, status.Error(codes.ResourceExhausted, "private transport explanation")
		}}
		r := startTestReporter(t, client, context.Background(), 1, 1)
		if err := r.submit(context.Background(), reporterResult(1)); err != nil {
			t.Fatal(err)
		}
		r.stop()
		if err := r.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		calls := client.snapshot()
		if len(calls) != 7 {
			t.Fatalf("actual report calls = %d, want 7", len(calls))
		}
		steps := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 16 * time.Second}
		for i, step := range steps {
			delay := calls[i+1].started.Sub(calls[i].finished)
			if delay < step*8/10 || delay > step*12/10 {
				t.Fatalf("retry %d completion-based backoff = %v, base = %v", i+1, delay, step)
			}
		}
		for _, call := range calls {
			deadline, ok := call.ctx.Deadline()
			if !ok || deadline.Sub(call.started) != 2*time.Second {
				t.Fatal("report call did not receive its independent two-second budget")
			}
			if call.sendLimit != reportMessageLimit {
				t.Fatal("actual RPC send limit differed from the preflight size check")
			}
		}
		time.Sleep(time.Hour)
		if len(client.snapshot()) != 7 {
			t.Fatal("exhausted result restarted or had an independent TTL loop")
		}
	})
}

func TestReporterBackoffReleasesWorkerAndDueRetryPrecedesNewResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		defer releaseOnce()
		client := &reportClientStub{call: func(_ context.Context, request *protocol.ReportTaskResultRequest, attempt int) (*protocol.ReportTaskResultResponse, error) {
			if request.TaskId == 1 && attempt == 1 {
				return nil, status.Error(codes.Unavailable, "unavailable")
			}
			if request.TaskId == 2 {
				// Hold the only worker until the old retry becomes due.
				<-release
			}
			return &protocol.ReportTaskResultResponse{}, nil
		}}
		r := startTestReporter(t, client, context.Background(), 3, 1)
		if err := r.submit(context.Background(), reporterResult(1)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := r.submit(context.Background(), reporterResult(2)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if got := reportTaskIDs(client.snapshot()); !reflect.DeepEqual(got, []int64{1, 2}) {
			t.Fatalf("backoff occupied the only worker: %v", got)
		}
		time.Sleep(1500 * time.Millisecond)
		if err := r.submit(context.Background(), reporterResult(3)); err != nil {
			t.Fatal(err)
		}
		r.stop()
		releaseOnce()
		if err := r.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := reportTaskIDs(client.snapshot()); !reflect.DeepEqual(got, []int64{1, 2, 1, 3}) {
			t.Fatalf("new result overtook an already due retry: %v", got)
		}
	})
}

func TestReporterPermanentFailuresUseCodesAndStableBusinessReason(t *testing.T) {
	invalidBusiness := reportStatus(t, codes.Code(bizerror.DefaultCode), subcode.InvalidRequest)
	standardWithReason := reportStatus(t, codes.InvalidArgument, subcode.InvalidRequest)
	otherBusiness := reportStatus(t, codes.Code(bizerror.DefaultCode), subcode.ResourceNotFound)
	for _, test := range []struct {
		name     string
		response *protocol.ReportTaskResultResponse
		err      error
		calls    int
	}{
		{"ack", &protocol.ReportTaskResultResponse{}, nil, 1},
		{"nil response is not ack", nil, nil, 7},
		{"unauthenticated", nil, status.Error(codes.Unauthenticated, "private"), 1},
		{"permission denied", nil, status.Error(codes.PermissionDenied, "private"), 1},
		{"stable invalid request", nil, invalidBusiness, 1},
		{"standard reason is not business rejection", nil, standardWithReason, 7},
		{"message does not identify rejection", nil, status.Error(codes.Unknown, subcode.InvalidRequest), 7},
		{"other business failure", nil, otherBusiness, 7},
		{"response with error is not ack", &protocol.ReportTaskResultResponse{}, status.Error(codes.Unavailable, "private"), 7},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &reportClientStub{call: func(context.Context, *protocol.ReportTaskResultRequest, int) (*protocol.ReportTaskResultResponse, error) {
					return test.response, test.err
				}}
				r := startTestReporter(t, client, context.Background(), 1, 1)
				if err := r.submit(context.Background(), reporterResult(1)); err != nil {
					t.Fatal(err)
				}
				r.stop()
				if err := r.wait(context.Background()); err != nil {
					t.Fatal(err)
				}
				if got := len(client.snapshot()); got != test.calls {
					t.Fatalf("calls = %d, want %d", got, test.calls)
				}
			})
		})
	}
}

func TestReporterOutputPreservesWireMeaningAndRejectsMalformedSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &reportClientStub{}
		r := startTestReporter(t, client, context.Background(), 12, 1)
		results := []executionResult{
			reporterResult(1),
			{jobID: 11, instanceID: 22, taskID: 2, attemptNo: 3, success: true, data: json.RawMessage("null")},
			{jobID: 11, instanceID: 22, taskID: 3, attemptNo: 3, success: true, prepared: true, preparedItems: []json.RawMessage{}},
			{jobID: 11, instanceID: 22, taskID: 4, attemptNo: 3, success: true, prepared: true, preparedItems: []json.RawMessage{json.RawMessage("9007199254740993"), json.RawMessage("null")}},
			{jobID: 11, instanceID: 22, taskID: 5, attemptNo: 3, success: true, data: json.RawMessage("9007199254740993")},
			{jobID: 11, instanceID: 22, taskID: 6, attemptNo: 3, success: false, prepared: true, data: json.RawMessage("bad"), preparedItems: []json.RawMessage{nil}},
			{taskID: 7, success: true, data: json.RawMessage{}},
			{taskID: 8, success: true, data: json.RawMessage("bad")},
			{taskID: 9, success: true, prepared: true},
			{taskID: 10, success: true, prepared: true, preparedItems: []json.RawMessage{nil}},
			{taskID: 11, success: true, summary: string([]byte{0xff})},
		}
		for _, result := range results {
			if err := r.submit(context.Background(), result); err != nil {
				t.Fatal(err)
			}
		}
		r.stop()
		if err := r.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		calls := client.snapshot()
		if len(calls) != 6 {
			t.Fatalf("malformed successful outputs were sent or converted to business failure: %d calls", len(calls))
		}
		for _, call := range calls {
			request := call.request
			if request.JobId != 11 || request.InstanceId != 22 || request.AttemptNo != 3 ||
				request.ExecutorInstanceId != adapterTestID || request.Auth.GetGroupName() != adapterTestGroup || request.Auth.GetToken() != adapterTestToken {
				t.Fatal("result transport lost attempt identity or component credentials")
			}
		}
		if calls[0].request.Output != nil {
			t.Fatal("nil Data unexpectedly selected a transport output")
		}
		if string(calls[1].request.GetDataJson()) != "null" {
			t.Fatal("explicit JSON null was omitted")
		}
		if calls[2].request.GetPreparedItems() == nil || len(calls[2].request.GetPreparedItems().Items) != 0 {
			t.Fatal("successful empty preparation lost its explicit output branch")
		}
		if string(calls[3].request.GetPreparedItems().Items[0]) != "9007199254740993" ||
			string(calls[4].request.GetDataJson()) != "9007199254740993" {
			t.Fatal("raw JSON numeric precision changed during reporting")
		}
		if calls[5].request.Outcome != protocol.TaskOutcome_FAILURE || calls[5].request.Output != nil {
			t.Fatal("business failure validated or reported unused output")
		}
	})
}

func TestReportMessageProblemUsesEncodedSizeAndRejectsInvalidStrings(t *testing.T) {
	request := &protocol.ReportTaskResultRequest{
		Auth:  &protocol.GroupAuth{GroupName: adapterTestGroup, Token: adapterTestToken},
		JobId: 11, InstanceId: 22, TaskId: 33, AttemptNo: 4,
		ExecutorInstanceId: adapterTestID, Summary: "valid Unicode \u2713",
		Output: &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte(`{"count":9007199254740993}`)},
	}
	size := proto.Size(request)
	if reason := reportMessageProblem(request, size); reason != "" {
		t.Fatalf("exact encoded limit rejected: %s", reason)
	}
	if reason := reportMessageProblem(request, size+1); reason != "" {
		t.Fatalf("below-limit request rejected: %s", reason)
	}
	if reason := reportMessageProblem(request, size-1); reason != "message_too_large" {
		t.Fatalf("complete protobuf size was not used: %s", reason)
	}
	for _, invalid := range []func(*protocol.ReportTaskResultRequest){
		func(r *protocol.ReportTaskResultRequest) { r.Summary = string([]byte{0xff}) },
		func(r *protocol.ReportTaskResultRequest) { r.ExecutorInstanceId = string([]byte{0xff}) },
		func(r *protocol.ReportTaskResultRequest) { r.Auth.GroupName = string([]byte{0xff}) },
		func(r *protocol.ReportTaskResultRequest) { r.Auth.Token = string([]byte{0xff}) },
	} {
		copy := proto.Clone(request).(*protocol.ReportTaskResultRequest)
		invalid(copy)
		if reason := reportMessageProblem(copy, size+100); reason != "invalid_encoding" {
			t.Fatalf("invalid protobuf string was not a definite encoding failure: %s", reason)
		}
	}
}

func TestReporterWaitRequiresActualClientReturnAndUsesRuntimeDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releaseOnce := sync.OnceFunc(func() { close(release) })
		defer releaseOnce()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		client := &reportClientStub{call: func(ctx context.Context, _ *protocol.ReportTaskResultRequest, _ int) (*protocol.ReportTaskResultResponse, error) {
			// Cancellation is not proof that an adapter has returned.
			<-release
			return nil, ctx.Err()
		}}
		r := startTestReporter(t, client, ctx, 1, 1)
		if err := r.submit(context.Background(), reporterResult(1)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		calls := client.snapshot()
		deadline, _ := calls[0].ctx.Deadline()
		if deadline.Sub(calls[0].started) != 500*time.Millisecond {
			t.Fatal("report call extended the remaining runtime budget")
		}
		time.Sleep(time.Second)
		if calls[0].ctx.Err() == nil {
			t.Fatal("runtime deadline did not cancel in-flight reporting")
		}
		if err := r.wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("wait claimed completion before the client returned: %v", err)
		}
		releaseOnce()
		if err := r.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(client.snapshot()) != 1 {
			t.Fatal("expired runtime retried a result")
		}
		if err := r.wait(ctx); err != nil {
			t.Fatalf("finished work did not take precedence over expired caller: %v", err)
		}
	})
}

func TestReporterDiscardDiagnosticsExcludeCredentialsPayloadsAndRawErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs reportLogBuffer
		previous := logx.Reset()
		logx.SetWriter(logx.NewWriter(&logs))
		t.Cleanup(func() { logx.SetWriter(previous) })
		client := &reportClientStub{call: func(context.Context, *protocol.ReportTaskResultRequest, int) (*protocol.ReportTaskResultResponse, error) {
			return nil, status.Error(codes.Unauthenticated, "synthetic-private-transport-detail")
		}}
		r := startTestReporter(t, client, context.Background(), 2, 1)
		valid := reporterResult(1)
		valid.summary, valid.data = "synthetic-private-summary", json.RawMessage(`"synthetic-private-result"`)
		invalid := reporterResult(2)
		invalid.data = json.RawMessage("synthetic-private-invalid-json")
		if err := r.submit(context.Background(), valid); err != nil {
			t.Fatal(err)
		}
		if err := r.submit(context.Background(), invalid); err != nil {
			t.Fatal(err)
		}
		r.stop()
		if err := r.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		output := logs.String()
		if !strings.Contains(output, "unauthenticated") || !strings.Contains(output, "invalid_output") {
			t.Fatal("discard diagnostics lost their fixed failure classification")
		}
		for _, secret := range []string{adapterTestToken, valid.summary, "synthetic-private-result", string(invalid.data), "synthetic-private-transport-detail"} {
			if strings.Contains(output, secret) {
				t.Fatal("reporting diagnostics leaked credential, output or raw transport error")
			}
		}
	})
}

func startTestReporter(t *testing.T, client protocol.ResultReceiverClient, parent context.Context, capacity, concurrency int) *reporter {
	t.Helper()
	auth, err := NewAuthenticator(adapterTestGroup, adapterTestToken)
	if err != nil {
		t.Fatal(err)
	}
	r, err := newReporter(client, auth, adapterTestID, capacity, concurrency)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(parent)
	if err := r.start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		r.stop()
		if err := r.wait(context.Background()); err != nil {
			t.Errorf("wait for reporting workers: %v", err)
		}
	})
	return r
}

func reporterResult(id int64) executionResult {
	return executionResult{jobID: 11, instanceID: 22, taskID: id, attemptNo: 3, success: true}
}

func reportStatus(t *testing.T, code codes.Code, reason string) error {
	t.Helper()
	st, err := status.New(code, "synthetic-private-rejection").WithDetails(&errdetails.ErrorInfo{Reason: reason})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

func reportTaskIDs(calls []reportCall) []int64 {
	ids := make([]int64, len(calls))
	for i, call := range calls {
		ids[i] = call.request.TaskId
	}
	return ids
}

type reportCall struct {
	ctx               context.Context
	request           *protocol.ReportTaskResultRequest
	started, finished time.Time
	sendLimit         int
}

// reportClientStub observes real worker scheduling and Contexts while controlling
// only the RPC outcome. Every method and mutable field belongs to this one fixture.
type reportClientStub struct {
	mu       sync.Mutex
	calls    []reportCall
	attempts map[int64]int
	call     func(context.Context, *protocol.ReportTaskResultRequest, int) (*protocol.ReportTaskResultResponse, error)
}

func (c *reportClientStub) ReportTaskResult(ctx context.Context, request *protocol.ReportTaskResultRequest, options ...grpc.CallOption) (*protocol.ReportTaskResultResponse, error) {
	c.mu.Lock()
	if c.attempts == nil {
		c.attempts = make(map[int64]int)
	}
	c.attempts[request.TaskId]++
	attempt, index := c.attempts[request.TaskId], len(c.calls)
	call := reportCall{ctx: ctx, request: proto.Clone(request).(*protocol.ReportTaskResultRequest), started: time.Now()}
	for _, option := range options {
		if size, ok := option.(grpc.MaxSendMsgSizeCallOption); ok {
			call.sendLimit = size.MaxSendMsgSize
		}
	}
	c.calls = append(c.calls, call)
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.calls[index].finished = time.Now()
	}()
	if c.call == nil {
		return &protocol.ReportTaskResultResponse{}, nil
	}
	return c.call(ctx, request, attempt)
}

func (c *reportClientStub) snapshot() []reportCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]reportCall(nil), c.calls...)
}

type reportClientFunction func(context.Context, *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error)

func (f reportClientFunction) ReportTaskResult(ctx context.Context, request *protocol.ReportTaskResultRequest, _ ...grpc.CallOption) (*protocol.ReportTaskResultResponse, error) {
	return f(ctx, request)
}

type reportLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *reportLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(data)
}

func (b *reportLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
