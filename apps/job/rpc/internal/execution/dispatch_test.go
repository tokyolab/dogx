package execution

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestNewSenderRejectsNilProviderWithoutResolving(t *testing.T) {
	var pointer *senderProviderStub
	var function senderProviderFunc
	for _, provider := range []ExecutorProvider{nil, pointer, function} {
		if sender, err := NewSender(provider, DefaultDispatchRPCTimeout); sender != nil || err == nil {
			t.Fatal("nil executor provider accepted")
		}
	}
	provider := &senderProviderStub{}
	if _, err := NewSender(provider, DefaultDispatchRPCTimeout); err != nil || len(provider.groups) != 0 {
		t.Fatalf("construction resolved an executor target: %v", err)
	}
}

func TestSenderRejectsInvalidTargetsBeforeRPC(t *testing.T) {
	var typedNil *senderClientStub
	for _, test := range []struct {
		name string
		edit func(*ExecutorTarget)
	}{
		{"wrong group", func(target *ExecutorTarget) { target.Group.ID++ }},
		{"nil client", func(target *ExecutorTarget) { target.Client = nil }},
		{"typed nil client", func(target *ExecutorTarget) { target.Client = typedNil }},
		{"invalid group name", func(target *ExecutorTarget) { target.Group.Name = " " }},
		{"invalid token", func(target *ExecutorTarget) { target.Group.Token = "synthetic-private-token" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &senderClientStub{}
			target := senderTarget(client)
			test.edit(&target)
			provider := &senderProviderStub{target: target}
			sender := newTestSender(t, provider)
			response, err := sender.Dispatch(context.Background(), senderAttempt())
			if err == nil || response.Outcome != dispatch.OutcomeUnknown || client.calls != 0 {
				t.Fatal("invalid target produced a known outcome or sent a request")
			}
			if strings.Contains(err.Error(), "synthetic-private-token") || strings.Contains(response.Summary, "synthetic-private-token") {
				t.Fatal("target validation exposed credential values")
			}
		})
	}
	failed := errors.New("synthetic-private-provider-detail")
	sender := newTestSender(t, &senderProviderStub{err: failed})
	response, err := sender.Dispatch(context.Background(), senderAttempt())
	if !errors.Is(err, failed) || response.Outcome != dispatch.OutcomeUnknown || strings.Contains(response.Summary, failed.Error()) {
		t.Fatal("provider failure lost its cause or leaked it into the persisted summary")
	}
}

func TestSenderUsesFrozenOrdinaryInputAndCurrentGroupCredentials(t *testing.T) {
	attempt := senderAttempt()
	before := attempt
	address := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9100}
	client := &senderClientStub{response: &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_ACCEPTED, ExecutorInstanceId: receiverTestID}, address: address}
	provider := &senderProviderStub{target: senderTarget(client)}
	sender := newTestSender(t, provider)
	response, err := sender.Dispatch(context.Background(), attempt)
	if err != nil || response.Outcome != dispatch.OutcomeAccepted || response.ExecutorAddress != address.String() ||
		response.ExecutorInstanceID != uuid.MustParse(receiverTestID) {
		t.Fatalf("valid admission ACK: outcome=%v error=%v", response.Outcome, err)
	}
	request := client.request
	if client.calls != 1 || !reflect.DeepEqual(provider.groups, []int64{attempt.ExecutorGroupID}) {
		t.Fatal("sender retried or resolved a different group")
	}
	if request.Auth.GetGroupName() != "test-workers" || request.Auth.GetToken() != receiverTestToken ||
		request.JobId != attempt.JobID || request.InstanceId != attempt.InstanceID || request.TaskId != attempt.TaskID ||
		request.AttemptNo != attempt.AttemptNo || request.Params != attempt.ExecutionConfig.Params ||
		request.ProcessorName != attempt.ExecutionConfig.ProcessorName || !request.DeadlineAt.AsTime().Equal(attempt.DeadlineAt) ||
		request.GetOrdinary() == nil || request.RequiredPost {
		t.Fatal("sender changed frozen execution facts or omitted protocol credentials")
	}
	if len(client.options) != 1 {
		t.Fatal("sender installed additional retry or waiting call options")
	}
	if _, ok := client.options[0].(grpc.PeerCallOption); !ok {
		t.Fatal("sender did not capture this dispatch's actual peer")
	}
	if !reflect.DeepEqual(attempt, before) || strings.Contains(response.Summary, attempt.ExecutionConfig.Params) {
		t.Fatal("sender changed the registered attempt or copied input into the summary")
	}
}

func TestSenderSeparatesLookupAndRPCBudgetsWithoutExtendingCaller(t *testing.T) {
	for _, test := range []struct {
		name           string
		caller, lookup time.Duration
		remaining      time.Duration
		configured     time.Duration
	}{
		{name: "default", remaining: 3 * time.Second},
		{name: "caller", caller: 500 * time.Millisecond, remaining: 500 * time.Millisecond},
		{name: "lookup uses overall budget", caller: 10 * time.Second, lookup: time.Second, remaining: 3 * time.Second},
		{name: "short remaining overall budget", caller: 2 * time.Second, lookup: time.Second, remaining: time.Second},
		{name: "configured admission budget", configured: 5 * time.Second, caller: 10 * time.Second, lookup: time.Second, remaining: 5 * time.Second},
		{name: "configured budget still clips", configured: 5 * time.Second, caller: 3 * time.Second, lookup: time.Second, remaining: 2 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &senderClientStub{response: &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_BUSY}}
				provider := &senderProviderStub{target: senderTarget(client), delay: test.lookup}
				sender := newTestSender(t, provider)
				if test.configured > 0 {
					var err error
					sender, err = NewSender(provider, test.configured)
					if err != nil {
						t.Fatal(err)
					}
				}
				ctx := context.Background()
				if test.caller > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, test.caller)
					defer cancel()
				}
				response, err := sender.Dispatch(ctx, senderAttempt())
				if err != nil || response.Outcome != dispatch.OutcomeBusy || client.budget != test.remaining {
					t.Fatalf("dispatch budget=%v outcome=%v error=%v", client.budget, response.Outcome, err)
				}
				if provider.budget != test.caller {
					t.Fatal("target lookup did not use the caller's independent overall budget")
				}
			})
		})
	}
	synctest.Test(t, func(t *testing.T) {
		client := &senderClientStub{}
		provider := &senderProviderStub{target: senderTarget(client), delay: time.Second}
		sender := newTestSender(t, provider)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		response, err := sender.Dispatch(ctx, senderAttempt())
		if !errors.Is(err, context.DeadlineExceeded) || response.Outcome != dispatch.OutcomeUnknown || client.calls != 0 {
			t.Fatal("expired lookup budget authorized a new RPC")
		}
	})
}

func TestSenderInvalidInputNeverResolvesOrSends(t *testing.T) {
	for _, edit := range []func(*dispatch.RegisteredAttempt){
		func(a *dispatch.RegisteredAttempt) { a.TaskID = 0 },
		func(a *dispatch.RegisteredAttempt) { a.ExecutorGroupID = 0 },
		func(a *dispatch.RegisteredAttempt) { a.AttemptNo = 0 },
		func(a *dispatch.RegisteredAttempt) { a.DeadlineAt = time.Time{} },
		func(a *dispatch.RegisteredAttempt) { a.DeadlineAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		func(a *dispatch.RegisteredAttempt) { a.ExecutionConfig.ExecutionMode = model.ExecutionModeSharded },
		func(a *dispatch.RegisteredAttempt) { a.ExecutionConfig.PostEnabled = true },
		func(a *dispatch.RegisteredAttempt) { a.ExecutionConfig.SchemaVersion = 2 },
		func(a *dispatch.RegisteredAttempt) { a.ExecutionConfig.ProcessorName = " " },
		func(a *dispatch.RegisteredAttempt) { a.ExecutionConfig.Params = string([]byte{0xff}) },
	} {
		provider := &senderProviderStub{}
		sender := newTestSender(t, provider)
		attempt := senderAttempt()
		edit(&attempt)
		response, err := sender.Dispatch(context.Background(), attempt)
		if err == nil || response.Outcome != dispatch.OutcomeUnknown || len(provider.groups) != 0 {
			t.Fatal("invalid native attempt was resolved or treated as executor refusal")
		}
	}
	provider := &senderProviderStub{}
	sender := newTestSender(t, provider)
	if _, err := sender.Dispatch(nil, senderAttempt()); err == nil {
		t.Fatal("nil context accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sender.Dispatch(ctx, senderAttempt()); !errors.Is(err, context.Canceled) || len(provider.groups) != 0 {
		t.Fatal("canceled dispatch performed target lookup")
	}
}

func TestSenderOnlyRecognizesDocumentedPreAdmissionErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want dispatch.Outcome
	}{
		{"unauthenticated", status.Error(codes.Unauthenticated, "synthetic-private-detail"), dispatch.OutcomeRejected},
		{"permission denied", status.Error(codes.PermissionDenied, "synthetic-private-detail"), dispatch.OutcomeRejected},
		{"invalid request", senderStatus(t, codes.Code(bizerror.DefaultCode), subcode.InvalidRequest), dispatch.OutcomeRejected},
		{"dispatch rejected", senderStatus(t, codes.Code(bizerror.DefaultCode), protocol.DispatchRejected), dispatch.OutcomeRejected},
		{"unknown subcode", senderStatus(t, codes.Code(bizerror.DefaultCode), subcode.ResourceNotFound), dispatch.OutcomeUnknown},
		{"other numeric business code", senderStatus(t, codes.Code(bizerror.DefaultCode+1), protocol.DispatchRejected), dispatch.OutcomeUnknown},
		{"standard invalid argument", senderStatus(t, codes.InvalidArgument, subcode.InvalidRequest), dispatch.OutcomeUnknown},
		{"missing detail", status.Error(codes.Code(bizerror.DefaultCode), protocol.DispatchRejected), dispatch.OutcomeUnknown},
		{"busy text is not busy", status.Error(codes.ResourceExhausted, "BUSY"), dispatch.OutcomeUnknown},
		{"unavailable", status.Error(codes.Unavailable, protocol.DispatchRejected), dispatch.OutcomeUnknown},
		{"deadline", status.Error(codes.DeadlineExceeded, "expired"), dispatch.OutcomeUnknown},
		{"message is not a contract", errors.New(subcode.InvalidRequest), dispatch.OutcomeUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &senderClientStub{err: test.err}
			sender := newTestSender(t, &senderProviderStub{target: senderTarget(client)})
			response, err := sender.Dispatch(context.Background(), senderAttempt())
			if response.Outcome != test.want || client.calls != 1 {
				t.Fatalf("outcome=%v calls=%d", response.Outcome, client.calls)
			}
			if test.want == dispatch.OutcomeRejected && err != nil || test.want == dispatch.OutcomeUnknown && !errors.Is(err, test.err) {
				t.Fatal("sender lost the technical cause or rejected a known refusal incorrectly")
			}
			if strings.Contains(response.Summary, "synthetic-private-detail") || response.ExecutorAddress != "" || response.ExecutorInstanceID != uuid.Nil {
				t.Fatal("error mapping leaked diagnostics or produced an executor binding")
			}
		})
	}
}

func TestSenderAcceptedRequiresCanonicalIdentityAndActualTCPPeer(t *testing.T) {
	var nilTCP *net.TCPAddr
	for _, test := range []struct {
		name    string
		ack     *protocol.DispatchTaskResponse
		address net.Addr
	}{
		{name: "nil ack"},
		{name: "zero status", ack: &protocol.DispatchTaskResponse{}},
		{name: "unknown status", ack: &protocol.DispatchTaskResponse{Status: 99}},
		{name: "missing identity", ack: &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_ACCEPTED}},
		{name: "unhyphenated identity", ack: &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_ACCEPTED, ExecutorInstanceId: strings.ReplaceAll(receiverTestID, "-", "")}},
		{name: "wrong version", ack: &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_ACCEPTED, ExecutorInstanceId: "22222222-2222-1222-8222-222222222222"}},
		{name: "missing peer", ack: senderAcceptedACK()},
		{name: "typed nil peer", ack: senderAcceptedACK(), address: nilTCP},
		{name: "unix peer", ack: senderAcceptedACK(), address: &net.UnixAddr{Name: "/tmp/executor", Net: "unix"}},
		{name: "unspecified peer", ack: senderAcceptedACK(), address: &net.TCPAddr{IP: net.IPv4zero, Port: 9100}},
		{name: "missing IP", ack: senderAcceptedACK(), address: &net.TCPAddr{Port: 9100}},
		{name: "invalid port", ack: senderAcceptedACK(), address: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}},
		{name: "out of range port", ack: senderAcceptedACK(), address: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 65536}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &senderClientStub{response: test.ack, address: test.address}
			sender := newTestSender(t, &senderProviderStub{target: senderTarget(client)})
			response, err := sender.Dispatch(context.Background(), senderAttempt())
			if err == nil || response.Outcome != dispatch.OutcomeUnknown || response.ExecutorAddress != "" || response.ExecutorInstanceID != uuid.Nil || client.calls != 1 {
				t.Fatal("invalid acknowledgement produced a binding, definite refusal or retry")
			}
		})
	}
	client := &senderClientStub{response: &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_BUSY}}
	response, err := newTestSender(t, &senderProviderStub{target: senderTarget(client)}).Dispatch(context.Background(), senderAttempt())
	if err != nil || response.Outcome != dispatch.OutcomeBusy || response.ExecutorAddress != "" || response.ExecutorInstanceID != uuid.Nil {
		t.Fatal("explicit BUSY incorrectly required admission identity or a binding")
	}
}

func newTestSender(t *testing.T, provider ExecutorProvider) *Sender {
	t.Helper()
	sender, err := NewSender(provider, DefaultDispatchRPCTimeout)
	if err != nil {
		t.Fatal(err)
	}
	return sender
}

func senderAttempt() dispatch.RegisteredAttempt {
	return dispatch.RegisteredAttempt{JobID: 11, InstanceID: 22, ExecutorGroupID: 41, TaskID: 33, AttemptNo: 2,
		ExecutionConfig: model.ExecutionConfig{SchemaVersion: 1, ExecutionMode: model.ExecutionModeNormal,
			ProcessorName: "test.frozen", Params: "synthetic-private-frozen-input"},
		RegisteredAt: time.Now(), DeadlineAt: time.Now().Add(time.Hour)}
}

func senderTarget(client protocol.ExecutorClient) ExecutorTarget {
	return ExecutorTarget{Group: repository.ExecutorGroupConfig{ID: 41, Name: "test-workers", Token: receiverTestToken,
		DiscoveryKey: "/test/executor-sender"}, Client: client}
}

func senderAcceptedACK() *protocol.DispatchTaskResponse {
	return &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_ACCEPTED, ExecutorInstanceId: receiverTestID}
}

func senderStatus(t *testing.T, code codes.Code, reason string) error {
	t.Helper()
	st, err := status.New(code, "synthetic-private-detail").WithDetails(&errdetails.ErrorInfo{Reason: reason})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

type senderProviderStub struct {
	target ExecutorTarget
	err    error
	groups []int64
	delay  time.Duration
	budget time.Duration
}

func (p *senderProviderStub) Executor(ctx context.Context, groupID int64) (ExecutorTarget, error) {
	p.groups = append(p.groups, groupID)
	if deadline, ok := ctx.Deadline(); ok {
		p.budget = time.Until(deadline)
	}
	if p.delay != 0 {
		time.Sleep(p.delay)
	}
	return p.target, p.err
}

type senderProviderFunc func(context.Context, int64) (ExecutorTarget, error)

func (f senderProviderFunc) Executor(ctx context.Context, id int64) (ExecutorTarget, error) {
	return f(ctx, id)
}

// senderClientStub implements the whole borrowed client while exposing the exact
// per-call options, so accidental dispatch retries or cancellation calls fail tests.
type senderClientStub struct {
	response *protocol.DispatchTaskResponse
	err      error
	address  net.Addr
	calls    int
	request  *protocol.DispatchTaskRequest
	options  []grpc.CallOption
	budget   time.Duration
}

func (c *senderClientStub) DispatchTask(ctx context.Context, request *protocol.DispatchTaskRequest, options ...grpc.CallOption) (*protocol.DispatchTaskResponse, error) {
	c.calls++
	c.request, c.options = request, options
	deadline, _ := ctx.Deadline()
	c.budget = time.Until(deadline)
	for _, option := range options {
		if destination, ok := option.(grpc.PeerCallOption); ok {
			*destination.PeerAddr = peer.Peer{Addr: c.address}
		}
	}
	return c.response, c.err
}

func (*senderClientStub) CancelInstance(context.Context, *protocol.CancelInstanceRequest, ...grpc.CallOption) (*protocol.CancelInstanceResponse, error) {
	panic("dispatch sender must not send cancellation")
}
