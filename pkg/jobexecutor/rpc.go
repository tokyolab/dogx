package jobexecutor

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrNotReady means new work was refused before admission because the
	// execution runtime is not ready. It must not describe an uncertain result.
	ErrNotReady = errors.New("executor is not ready")
	// ErrStopping means new work was refused before admission during shutdown.
	// An active duplicate must still be acknowledged instead of returning it.
	ErrStopping = errors.New("executor is stopping")
	// ErrExpired means a new attempt was refused before admission because its
	// original Instance deadline has passed. It is not an RPC timeout.
	ErrExpired = errors.New("executor deadline has expired")
)

// Stage identifies the processor operation selected by a dispatch request.
// It is independent of the transport's oneof and of persisted Task states.
type Stage uint8

const (
	StageOrdinary Stage = iota + 1
	StagePrepare
	StageShard
	StagePost
)

// CancelReason describes the scheduler's existing Instance stop decision.
// Cancellation of the RPC Context is not an Instance cancellation reason.
type CancelReason uint8

const (
	CancelManualStop CancelReason = iota + 1
	CancelInstanceTimeout
)

// Invocation is a validated, credential-free attempt offered to the runtime.
// Input and DeadlineAt are frozen for the Instance. Items belongs only to a
// shard stage; the three counts belong only to Post. The adapter owns a copy of
// every item, so admitted work may retain it after the RPC has returned.
type Invocation struct {
	Input
	AttemptNo     int64
	ProcessorName string
	DeadlineAt    time.Time
	RequiredPost  bool
	Stage         Stage
	Items         []json.RawMessage
	Total         int64
	Succeeded     int64
	Failed        int64
}

// Admission owns local admission, active-attempt deduplication and cancellation.
// Implementations must be safe for concurrent calls. They receive no transport
// DTOs or credentials and must not return RPC status codes or business subcodes.
type Admission interface {
	// Accept returns (true, nil) only after this attempt is admitted or found
	// already active. It returns (false, nil) only for definite capacity refusal.
	// ErrNotReady, ErrStopping and ErrExpired guarantee no new work was admitted.
	// Other errors leave admission uncertain. Returning true with an error is
	// invalid and must never be interpreted as a definite refusal.
	//
	// An active duplicate takes precedence over readiness, capacity and expiry.
	// These decisions must be coordinated inside the runtime. The Context bounds
	// admission only; accepted business work uses the runtime's own lifecycle
	// and the original DeadlineAt, independent of the dispatch RPC's lifetime.
	Accept(ctx context.Context, invocation Invocation) (bool, error)
	// Cancel marks all current local activities for the Instance as cancelled
	// or signals their business Contexts before returning nil. No activity and
	// repeated cancellation are successful. It does not wait for business exit
	// or result delivery. Queued-to-running transitions must coordinate with it.
	Cancel(ctx context.Context, instanceID int64, reason CancelReason) error
}

// RPCAdapter authenticates executor methods and translates wire requests into
// native runtime operations. It neither executes processors nor acknowledges
// work before Admission does. Register it with bizerror.UnaryServerInterceptor
// and disable request/response body logging for both methods.
type RPCAdapter struct {
	protocol.UnimplementedExecutorServer
	auth               *Authenticator
	registry           *Registry
	executorInstanceID string
	admission          Admission
}

// NewRPCAdapter validates its dependencies and freezes processor registration.
// The process identity must be a canonical UUIDv4 generated once by the host.
// Constructing an adapter does not start a server or make the runtime ready.
func NewRPCAdapter(auth *Authenticator, registry *Registry, executorInstanceID string, admission Admission) (*RPCAdapter, error) {
	if auth == nil {
		return nil, errors.New("executor authenticator must not be nil")
	}
	if registry == nil {
		return nil, errors.New("executor registry must not be nil")
	}
	if !validExecutorInstanceID(executorInstanceID) {
		return nil, errors.New("executor process identity is invalid")
	}
	if nilAdmission(admission) {
		return nil, errors.New("executor admission must not be nil")
	}
	registry.Freeze()
	return &RPCAdapter{auth: auth, registry: registry, executorInstanceID: executorInstanceID, admission: admission}, nil
}

// DispatchTask acknowledges admission, not business completion. Dynamic expiry,
// shutdown and duplicate decisions remain atomic inside Admission; a short RPC
// deadline cannot change the lifetime of work which has already been accepted.
func (s *RPCAdapter) DispatchTask(ctx context.Context, req *protocol.DispatchTaskRequest) (*protocol.DispatchTaskResponse, error) {
	if err := s.authenticate(req.GetAuth()); err != nil {
		return nil, err
	}
	invocation, err := s.invocation(req)
	if err != nil {
		return nil, err
	}
	accepted, err := s.admission.Accept(ctx, invocation)
	if err != nil {
		if !accepted {
			switch {
			case errors.Is(err, ErrNotReady):
				return nil, rejectedDispatch("executor is not ready")
			case errors.Is(err, ErrStopping):
				return nil, rejectedDispatch("executor is stopping")
			case errors.Is(err, ErrExpired):
				return nil, rejectedDispatch("executor deadline has expired")
			}
		}
		return nil, executorTechnicalError(err, "executor admission failed")
	}
	if !accepted {
		return &protocol.DispatchTaskResponse{Status: protocol.DispatchStatus_BUSY}, nil
	}
	return &protocol.DispatchTaskResponse{
		Status: protocol.DispatchStatus_ACCEPTED, ExecutorInstanceId: s.executorInstanceID,
	}, nil
}

// CancelInstance completes local cancellation signalling, including the case
// where no matching work exists. It does not wait for business code to exit and
// never turns a cancellation failure into a dispatch refusal.
func (s *RPCAdapter) CancelInstance(ctx context.Context, req *protocol.CancelInstanceRequest) (*protocol.CancelInstanceResponse, error) {
	if err := s.authenticate(req.GetAuth()); err != nil {
		return nil, err
	}
	if req.InstanceId <= 0 {
		return nil, invalidExecutionRequest("instance ID must be positive")
	}
	var reason CancelReason
	switch req.Reason {
	case protocol.CancelReason_MANUAL_STOP:
		reason = CancelManualStop
	case protocol.CancelReason_INSTANCE_TIMEOUT:
		reason = CancelInstanceTimeout
	default:
		return nil, invalidExecutionRequest("cancellation reason is invalid")
	}
	if err := s.admission.Cancel(ctx, req.InstanceId, reason); err != nil {
		return nil, executorTechnicalError(err, "executor cancellation failed")
	}
	return &protocol.CancelInstanceResponse{}, nil
}

func (s *RPCAdapter) authenticate(auth *protocol.GroupAuth) error {
	return s.auth.Authenticate(auth.GetGroupName(), auth.GetToken())
}

func (s *RPCAdapter) invocation(req *protocol.DispatchTaskRequest) (Invocation, error) {
	if req.JobId <= 0 || req.InstanceId <= 0 || req.TaskId <= 0 || req.AttemptNo <= 0 {
		return Invocation{}, invalidExecutionRequest("execution IDs and attempt number must be positive")
	}
	if !validExecutionName(req.ProcessorName) {
		return Invocation{}, invalidExecutionRequest("processor name is invalid")
	}
	if req.DeadlineAt == nil || req.DeadlineAt.CheckValid() != nil {
		return Invocation{}, invalidExecutionRequest("execution deadline is invalid")
	}
	invocation := Invocation{
		Input:     Input{JobID: req.JobId, InstanceID: req.InstanceId, TaskID: req.TaskId, Params: req.Params},
		AttemptNo: req.AttemptNo, ProcessorName: req.ProcessorName,
		DeadlineAt: req.DeadlineAt.AsTime(), RequiredPost: req.RequiredPost,
	}
	switch input := req.Input.(type) {
	case *protocol.DispatchTaskRequest_Ordinary:
		if input == nil || input.Ordinary == nil || req.RequiredPost {
			return Invocation{}, invalidExecutionRequest("ordinary input is invalid")
		}
		invocation.Stage = StageOrdinary
	case *protocol.DispatchTaskRequest_Prepare:
		if input == nil || input.Prepare == nil {
			return Invocation{}, invalidExecutionRequest("prepare input is required")
		}
		invocation.Stage = StagePrepare
	case *protocol.DispatchTaskRequest_Shard:
		if input == nil || input.Shard == nil {
			return Invocation{}, invalidExecutionRequest("shard input is required")
		}
		invocation.Stage = StageShard
		invocation.Items = make([]json.RawMessage, len(input.Shard.Items))
		for index, item := range input.Shard.Items {
			if !json.Valid(item) {
				return Invocation{}, invalidExecutionRequest("shard item must contain valid JSON")
			}
			invocation.Items[index] = append(json.RawMessage(nil), item...)
		}
	case *protocol.DispatchTaskRequest_Post:
		if input == nil || input.Post == nil {
			return Invocation{}, invalidExecutionRequest("post input is required")
		}
		if !req.RequiredPost {
			return Invocation{}, invalidExecutionRequest("post execution must be enabled")
		}
		post := input.Post
		if post.Total < 0 || post.Succeeded < 0 || post.Failed < 0 || post.Succeeded > post.Total || post.Failed != post.Total-post.Succeeded {
			return Invocation{}, invalidExecutionRequest("post counts are invalid")
		}
		invocation.Stage = StagePost
		invocation.Total, invocation.Succeeded, invocation.Failed = post.Total, post.Succeeded, post.Failed
	default:
		return Invocation{}, invalidExecutionRequest("execution input is required")
	}
	if invocation.Stage == StageOrdinary {
		if _, ok := s.registry.Lookup(req.ProcessorName); !ok {
			return Invocation{}, rejectedDispatch("ordinary processor is unavailable")
		}
	} else {
		processor, ok := s.registry.LookupSharding(req.ProcessorName)
		if !ok {
			return Invocation{}, rejectedDispatch("sharding processor is unavailable")
		}
		if req.RequiredPost || invocation.Stage == StagePost {
			if _, ok := processor.(PostProcessor); !ok {
				return Invocation{}, rejectedDispatch("post processor is unavailable")
			}
		}
	}
	return invocation, nil
}

func rejectedDispatch(message string) error {
	return bizerror.New(protocol.DispatchRejected, message)
}

func invalidExecutionRequest(message string) error {
	return bizerror.New(subcode.InvalidRequest, message)
}

func executorTechnicalError(err error, message string) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "executor request was cancelled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "executor request deadline was exceeded")
	}
	return status.Error(codes.Internal, message)
}

func nilAdmission(admission Admission) bool {
	if admission == nil {
		return true
	}
	value := reflect.ValueOf(admission)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
