package jobexecutor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type adapterAdmissionStub struct {
	accepted     bool
	err          error
	cancelErr    error
	invocations  []Invocation
	cancelledIDs []int64
	reasons      []CancelReason
}

func (s *adapterAdmissionStub) Accept(_ context.Context, invocation Invocation) (bool, error) {
	s.invocations = append(s.invocations, invocation)
	return s.accepted, s.err
}

func (s *adapterAdmissionStub) Cancel(_ context.Context, instanceID int64, reason CancelReason) error {
	s.cancelledIDs = append(s.cancelledIDs, instanceID)
	s.reasons = append(s.reasons, reason)
	return s.cancelErr
}

type adapterOrdinaryProcessor struct{}

func (adapterOrdinaryProcessor) Execute(context.Context, Input) (Result, error) {
	panic("the RPC adapter must not execute business work")
}

type adapterShardingProcessor struct{}

func (adapterShardingProcessor) PreProcess(context.Context, Input) (PrepareResult, error) {
	panic("the RPC adapter must not execute business work")
}

func (adapterShardingProcessor) ShardingProcess(context.Context, ShardInput) (Result, error) {
	panic("the RPC adapter must not execute business work")
}

type adapterPostProcessor struct{ adapterShardingProcessor }

func (adapterPostProcessor) PostProcess(context.Context, PostInput) (Result, error) {
	panic("the RPC adapter must not execute business work")
}

func newTestRPCAdapter(t *testing.T, admission Admission) *RPCAdapter {
	t.Helper()
	auth, err := NewAuthenticator(adapterTestGroup, adapterTestToken)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register("ordinary", adapterOrdinaryProcessor{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterSharding("sharding", adapterShardingProcessor{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterSharding("post", adapterPostProcessor{}); err != nil {
		t.Fatal(err)
	}
	adapter, err := NewRPCAdapter(auth, registry, adapterTestID, admission)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func validAdapterDispatch() *protocol.DispatchTaskRequest {
	return &protocol.DispatchTaskRequest{
		Auth:  &protocol.GroupAuth{GroupName: adapterTestGroup, Token: adapterTestToken},
		JobId: 1, InstanceId: 2, TaskId: 3, AttemptNo: 4, ProcessorName: "ordinary",
		Params: "  frozen parameters  ", DeadlineAt: timestamppb.New(time.Now().Add(time.Hour)),
		Input: &protocol.DispatchTaskRequest_Ordinary{Ordinary: &protocol.OrdinaryInput{}},
	}
}

func TestDispatchAdmissionAndErrorContract(t *testing.T) {
	for _, test := range []struct {
		name     string
		accepted bool
		err      error
		status   protocol.DispatchStatus
		code     codes.Code
		subcode  string
	}{
		{name: "accepted", accepted: true, status: protocol.DispatchStatus_ACCEPTED},
		{name: "capacity refusal", status: protocol.DispatchStatus_BUSY},
		{name: "not ready", err: ErrNotReady, code: codes.Code(bizerror.DefaultCode), subcode: protocol.DispatchRejected},
		{name: "stopping", err: fmt.Errorf("admit: %w", ErrStopping), code: codes.Code(bizerror.DefaultCode), subcode: protocol.DispatchRejected},
		{name: "expired", err: ErrExpired, code: codes.Code(bizerror.DefaultCode), subcode: protocol.DispatchRejected},
		{name: "unknown outcome", err: errors.New("unclassified technical fault"), code: codes.Internal},
		{name: "inconsistent outcome", accepted: true, err: ErrExpired, code: codes.Internal},
		{name: "request cancelled", err: context.Canceled, code: codes.Canceled},
		{name: "request deadline", err: context.DeadlineExceeded, code: codes.DeadlineExceeded},
		{name: "wrapped request context", err: fmt.Errorf("%s: %w", adapterTestToken, context.Canceled), code: codes.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := &adapterAdmissionStub{accepted: test.accepted, err: test.err}
			adapter := newTestRPCAdapter(t, runtime)
			request := validAdapterDispatch()
			response, err := bizerror.UnaryServerInterceptor(context.Background(), request, &grpc.UnaryServerInfo{},
				func(ctx context.Context, req any) (any, error) {
					return adapter.DispatchTask(ctx, req.(*protocol.DispatchTaskRequest))
				})
			if status.Code(err) != test.code {
				t.Fatalf("code = %v, want %v: %v", status.Code(err), test.code, err)
			}
			if err != nil && strings.Contains(err.Error(), adapterTestToken) {
				t.Fatal("reply leaked wrapped dependency diagnostics")
			}
			if len(runtime.invocations) != 1 {
				t.Fatal("request was not delegated exactly once")
			}
			if test.subcode != "" {
				reason, ok := bizerror.SubcodeFromStatus(status.Convert(err))
				if !ok || reason != test.subcode {
					t.Fatalf("reason = %q, want %q", reason, test.subcode)
				}
			}
			if err == nil {
				ack := response.(*protocol.DispatchTaskResponse)
				if ack.Status != test.status {
					t.Fatalf("status = %v, want %v", ack.Status, test.status)
				}
				if test.accepted && ack.ExecutorInstanceId != adapterTestID {
					t.Fatal("accepted response omitted process identity")
				}
				if !test.accepted && ack.ExecutorInstanceId != "" {
					t.Fatal("BUSY must not bind a process identity")
				}
			}
		})
	}
}

func TestDispatchLeavesActiveDuplicateAndExpiryToRuntime(t *testing.T) {
	runtime := &adapterAdmissionStub{accepted: true}
	adapter := newTestRPCAdapter(t, runtime)
	req := validAdapterDispatch()
	req.DeadlineAt = timestamppb.New(time.Now().Add(-time.Hour))
	for range 2 {
		ack, err := adapter.DispatchTask(context.Background(), req)
		if err != nil || ack.Status != protocol.DispatchStatus_ACCEPTED {
			t.Fatalf("active duplicate was rejected before runtime decision: %v", err)
		}
	}
	if len(runtime.invocations) != 2 || runtime.invocations[0].DeadlineAt != req.DeadlineAt.AsTime() || runtime.invocations[0].Params != req.Params {
		t.Fatal("adapter changed frozen input or made its own duplicate decision")
	}
}

func TestDispatchValidatesWireAndCapabilityBeforeAdmission(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*protocol.DispatchTaskRequest)
		subcode string
	}{
		{"zero ID", func(r *protocol.DispatchTaskRequest) { r.TaskId = 0 }, subcode.InvalidRequest},
		{"invalid attempt", func(r *protocol.DispatchTaskRequest) { r.AttemptNo = -1 }, subcode.InvalidRequest},
		{"processor whitespace", func(r *protocol.DispatchTaskRequest) { r.ProcessorName = " ordinary" }, subcode.InvalidRequest},
		{"processor too long", func(r *protocol.DispatchTaskRequest) { r.ProcessorName = strings.Repeat("界", 129) }, subcode.InvalidRequest},
		{"missing deadline", func(r *protocol.DispatchTaskRequest) { r.DeadlineAt = nil }, subcode.InvalidRequest},
		{"invalid deadline", func(r *protocol.DispatchTaskRequest) { r.DeadlineAt.Nanos = -1 }, subcode.InvalidRequest},
		{"missing stage", func(r *protocol.DispatchTaskRequest) { r.Input = nil }, subcode.InvalidRequest},
		{"nil ordinary", func(r *protocol.DispatchTaskRequest) { r.Input = &protocol.DispatchTaskRequest_Ordinary{} }, subcode.InvalidRequest},
		{"typed nil stage", func(r *protocol.DispatchTaskRequest) { r.Input = (*protocol.DispatchTaskRequest_Ordinary)(nil) }, subcode.InvalidRequest},
		{"ordinary post", func(r *protocol.DispatchTaskRequest) { r.RequiredPost = true }, subcode.InvalidRequest},
		{"missing processor", func(r *protocol.DispatchTaskRequest) { r.ProcessorName = "missing" }, protocol.DispatchRejected},
		{"wrong processor kind", func(r *protocol.DispatchTaskRequest) { r.ProcessorName = "sharding" }, protocol.DispatchRejected},
		{"missing prepare", func(r *protocol.DispatchTaskRequest) { r.Input = &protocol.DispatchTaskRequest_Prepare{} }, subcode.InvalidRequest},
		{"missing shard", func(r *protocol.DispatchTaskRequest) { r.Input = &protocol.DispatchTaskRequest_Shard{} }, subcode.InvalidRequest},
		{"missing post", func(r *protocol.DispatchTaskRequest) { r.Input = &protocol.DispatchTaskRequest_Post{} }, subcode.InvalidRequest},
		{"invalid item", func(r *protocol.DispatchTaskRequest) {
			r.Input = &protocol.DispatchTaskRequest_Shard{Shard: &protocol.ShardInput{Items: [][]byte{[]byte("{")}}}
		}, subcode.InvalidRequest},
		{"prepare needs post", func(r *protocol.DispatchTaskRequest) {
			r.ProcessorName = "sharding"
			r.RequiredPost = true
			r.Input = &protocol.DispatchTaskRequest_Prepare{Prepare: &protocol.PrepareInput{}}
		}, protocol.DispatchRejected},
		{"post not enabled", func(r *protocol.DispatchTaskRequest) {
			r.ProcessorName = "post"
			r.Input = &protocol.DispatchTaskRequest_Post{Post: &protocol.PostInput{}}
		}, subcode.InvalidRequest},
		{"post capability missing", func(r *protocol.DispatchTaskRequest) {
			r.ProcessorName = "sharding"
			r.RequiredPost = true
			r.Input = &protocol.DispatchTaskRequest_Post{Post: &protocol.PostInput{}}
		}, protocol.DispatchRejected},
		{"counts do not add up", func(r *protocol.DispatchTaskRequest) {
			r.ProcessorName = "post"
			r.RequiredPost = true
			r.Input = &protocol.DispatchTaskRequest_Post{Post: &protocol.PostInput{Total: 2, Succeeded: 1}}
		}, subcode.InvalidRequest},
		{"negative counts", func(r *protocol.DispatchTaskRequest) {
			r.ProcessorName = "post"
			r.RequiredPost = true
			r.Input = &protocol.DispatchTaskRequest_Post{Post: &protocol.PostInput{Total: -1}}
		}, subcode.InvalidRequest},
		{"overflow counts", func(r *protocol.DispatchTaskRequest) {
			r.ProcessorName = "post"
			r.RequiredPost = true
			r.Input = &protocol.DispatchTaskRequest_Post{Post: &protocol.PostInput{Total: 1, Succeeded: math.MaxInt64, Failed: math.MaxInt64}}
		}, subcode.InvalidRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := &adapterAdmissionStub{accepted: true}
			adapter := newTestRPCAdapter(t, runtime)
			req := validAdapterDispatch()
			test.mutate(req)
			_, err := adapter.DispatchTask(context.Background(), req)
			business, ok := bizerror.From(err)
			if !ok || business.Subcode() != test.subcode {
				t.Fatalf("error = %v, want %s", err, test.subcode)
			}
			if len(runtime.invocations) != 0 {
				t.Fatal("invalid request reached admission")
			}
		})
	}
}

func TestDispatchNativeStagesAndJSONOwnership(t *testing.T) {
	runtime := &adapterAdmissionStub{accepted: true}
	adapter := newTestRPCAdapter(t, runtime)
	req := validAdapterDispatch()
	req.ProcessorName, req.RequiredPost = "post", true
	req.Input = &protocol.DispatchTaskRequest_Prepare{Prepare: &protocol.PrepareInput{}}
	if _, err := adapter.DispatchTask(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	items := [][]byte{[]byte("9007199254740993"), []byte(`"\u0000"`)}
	req.Input = &protocol.DispatchTaskRequest_Shard{Shard: &protocol.ShardInput{Items: items}}
	if _, err := adapter.DispatchTask(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	items[0][0] = '1'
	items[1] = []byte("false")
	if string(runtime.invocations[1].Items[0]) != "9007199254740993" || string(runtime.invocations[1].Items[1]) != `"\u0000"` {
		t.Fatal("admission did not receive independent, lossless JSON")
	}
	req.Input = &protocol.DispatchTaskRequest_Post{Post: &protocol.PostInput{Total: math.MaxInt64, Succeeded: math.MaxInt64 - 1, Failed: 1}}
	if _, err := adapter.DispatchTask(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if runtime.invocations[0].Stage != StagePrepare || runtime.invocations[1].Stage != StageShard || runtime.invocations[2].Stage != StagePost || runtime.invocations[2].Failed != 1 {
		t.Fatal("stage conversion changed the invocation")
	}
}

func TestCancelAuthenticationAndLocalSemantics(t *testing.T) {
	runtime := &adapterAdmissionStub{}
	adapter := newTestRPCAdapter(t, runtime)
	if _, err := adapter.DispatchTask(context.Background(), nil); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("nil dispatch error = %v", err)
	}
	if _, err := adapter.CancelInstance(context.Background(), nil); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("nil cancellation error = %v", err)
	}
	req := &protocol.CancelInstanceRequest{Auth: validAdapterDispatch().Auth, InstanceId: 2, Reason: protocol.CancelReason_MANUAL_STOP}
	for range 2 {
		if _, err := adapter.CancelInstance(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if len(runtime.cancelledIDs) != 2 || runtime.reasons[0] != CancelManualStop {
		t.Fatal("repeated cancellation was not delegated")
	}
	req.Reason = protocol.CancelReason_INSTANCE_TIMEOUT
	if _, err := adapter.CancelInstance(context.Background(), req); err != nil || runtime.reasons[2] != CancelInstanceTimeout {
		t.Fatalf("timeout cancellation failed: %v", err)
	}
	req.InstanceId = 0
	_, err := adapter.CancelInstance(context.Background(), req)
	if business, ok := bizerror.From(err); !ok || business.Subcode() != subcode.InvalidRequest {
		t.Fatalf("invalid cancellation ID error = %v", err)
	}
	req.InstanceId, req.Reason = 2, protocol.CancelReason_CANCEL_REASON_UNSPECIFIED
	_, err = adapter.CancelInstance(context.Background(), req)
	if business, ok := bizerror.From(err); !ok || business.Subcode() != subcode.InvalidRequest {
		t.Fatalf("invalid cancellation reason error = %v", err)
	}
	req.Reason = protocol.CancelReason_MANUAL_STOP
	runtime.cancelErr = ErrStopping
	if _, err := adapter.CancelInstance(context.Background(), req); status.Code(err) != codes.Internal {
		t.Fatalf("cancellation reused dispatch rejection: %v", err)
	}
	req.Auth.Token = "wrong"
	if _, err := adapter.CancelInstance(context.Background(), req); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("invalid cancellation authentication error = %v", err)
	}
	if len(runtime.cancelledIDs) != 4 {
		t.Fatal("invalid cancellation reached the runtime")
	}
}

func TestNewRPCAdapterRejectsInvalidDependencies(t *testing.T) {
	auth, err := NewAuthenticator(adapterTestGroup, adapterTestToken)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &adapterAdmissionStub{}
	var typedNil *adapterAdmissionStub
	for _, test := range []struct {
		auth      *Authenticator
		registry  *Registry
		identity  string
		admission Admission
	}{
		{nil, NewRegistry(), adapterTestID, runtime},
		{auth, nil, adapterTestID, runtime},
		{auth, NewRegistry(), adapterTestID, nil},
		{auth, NewRegistry(), adapterTestID, typedNil},
		{auth, NewRegistry(), "22222222222242228222222222222222", runtime},
	} {
		if _, err := NewRPCAdapter(test.auth, test.registry, test.identity, test.admission); err == nil {
			t.Fatal("invalid adapter dependencies were accepted")
		}
	}
	registry := NewRegistry()
	if _, err := NewRPCAdapter(auth, registry, adapterTestID, runtime); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("late", adapterOrdinaryProcessor{}); !errors.Is(err, ErrRegistryFrozen) {
		t.Fatalf("adapter registry remained mutable: %v", err)
	}
}
