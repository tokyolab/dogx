package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/dispatch"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DefaultDispatchRPCTimeout is the admission-call default, excluding target
// lookup and business execution. An earlier caller deadline still takes precedence.
const DefaultDispatchRPCTimeout = 3 * time.Second

// ExecutorTarget borrows one group's immutable credentials and shared executor
// client. The provider owns connection creation, reuse and shutdown. Group and
// Client must describe the same discovery target; Group.Token is sensitive.
type ExecutorTarget struct {
	Group  repository.ExecutorGroupConfig
	Client protocol.ExecutorClient
}

// ExecutorProvider resolves the persisted Instance's executor group without
// changing it to the current Job's group. Implementations must support concurrent
// calls and honor ctx. A nil error requires a complete target whose connection
// is only borrowed by Sender. Group deletion may close it during an active call;
// that failure does not prove the attempt was rejected before admission.
type ExecutorProvider interface {
	Executor(context.Context, int64) (ExecutorTarget, error)
}

// Sender adapts one committed attempt to one executor RPC. It does not
// register attempts, retry dispatch, persist responses, or manage connections.
// Concurrent calls are safe if the provider and supplied clients are safe. The
// caller must send each newly registered attempt at most once; Sender retains no
// deduplication state. Returned errors preserve technical causes and can contain
// private upstream diagnostics: never log them or store them as result summaries.
type Sender struct {
	provider   ExecutorProvider
	rpcTimeout time.Duration
}

var _ dispatch.Sender = (*Sender)(nil)

// NewSender binds an existing target provider without resolving groups or opening
// connections. Nil and typed-nil providers are invalid. rpcTimeout must be at
// least one millisecond and match the borrowed client's configured timeout.
func NewSender(provider ExecutorProvider, rpcTimeout time.Duration) (*Sender, error) {
	if nilExecutorDependency(provider) {
		return nil, errors.New("executor target provider must not be nil")
	}
	if rpcTimeout < time.Millisecond {
		return nil, errors.New("executor dispatch RPC timeout must be at least one millisecond")
	}
	return &Sender{provider: provider, rpcTimeout: rpcTimeout}, nil
}

// Dispatch sends only the frozen stage input from attempt. Target lookup uses
// the caller's overall processing budget. The actual RPC has its configured
// limit, shortened by ctx's remaining budget. It makes one DispatchTask call,
// without WaitForReady or an application retry.
// Accepted, busy and recognized pre-admission refusals return nil error. Unknown
// responses and technical failures return OutcomeUnknown with an error, never
// evidence that an executor did not accept work. Summaries contain fixed platform
// descriptions only; an admission ACK is neither execution nor persistence proof.
func (s *Sender) Dispatch(ctx context.Context, attempt dispatch.RegisteredAttempt) (dispatch.Response, error) {
	unknown := dispatch.Response{Outcome: dispatch.OutcomeUnknown, Summary: "executor dispatch outcome is unknown"}
	if ctx == nil {
		return unknown, errors.New("executor dispatch context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return unknown, err
	}
	request, err := dispatchRequest(attempt)
	if err != nil {
		return unknown, err
	}
	target, err := s.provider.Executor(ctx, attempt.ExecutorGroupID)
	if err != nil {
		return unknown, fmt.Errorf("resolve executor target: %w", err)
	}
	if target.Group.ID != attempt.ExecutorGroupID || nilExecutorDependency(target.Client) {
		return unknown, errors.New("executor target does not match the registered group")
	}
	if err := jobexecutor.ValidateCredentials(target.Group.Name, target.Group.Token); err != nil {
		return unknown, fmt.Errorf("invalid executor target credentials: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return unknown, err
	}
	request.Auth = &protocol.GroupAuth{GroupName: target.Group.Name, Token: target.Group.Token}
	rpcCtx, cancel := context.WithTimeout(ctx, s.rpcTimeout)
	defer cancel()
	var remote peer.Peer
	ack, err := target.Client.DispatchTask(rpcCtx, request, grpc.Peer(&remote))
	if err != nil {
		if summary := dispatchRejectionSummary(err); summary != "" {
			return dispatch.Response{Outcome: dispatch.OutcomeRejected, Summary: summary}, nil
		}
		return unknown, fmt.Errorf("dispatch executor task: %w", err)
	}
	if ack == nil {
		return unknown, errors.New("executor returned no dispatch acknowledgement")
	}
	switch ack.Status {
	case protocol.DispatchStatus_BUSY:
		return dispatch.Response{Outcome: dispatch.OutcomeBusy, Summary: "executor is busy"}, nil
	case protocol.DispatchStatus_ACCEPTED:
		identity, err := uuid.Parse(ack.ExecutorInstanceId)
		if err != nil || identity.Version() != 4 || identity.Variant() != uuid.RFC4122 || identity.String() != ack.ExecutorInstanceId {
			return unknown, errors.New("executor acknowledgement has an invalid process identity")
		}
		address, ok := dispatchPeerAddress(remote.Addr)
		if !ok {
			return unknown, errors.New("executor acknowledgement has no valid TCP peer address")
		}
		return dispatch.Response{Outcome: dispatch.OutcomeAccepted, ExecutorInstanceID: identity,
			ExecutorAddress: address, Summary: "executor accepted dispatch"}, nil
	default:
		return unknown, errors.New("executor returned an unknown dispatch acknowledgement status")
	}
}

func dispatchRequest(attempt dispatch.RegisteredAttempt) (*protocol.DispatchTaskRequest, error) {
	config := attempt.ExecutionConfig
	if attempt.JobID <= 0 || attempt.InstanceID <= 0 || attempt.ExecutorGroupID <= 0 || attempt.TaskID <= 0 || attempt.AttemptNo <= 0 ||
		config.SchemaVersion != 1 || (config.ExecutionMode != model.ExecutionModeNormal && config.ExecutionMode != model.ExecutionModeSharded) ||
		(config.ExecutionMode == model.ExecutionModeNormal && config.PostEnabled) ||
		config.MaxRetries < 0 || config.RetryIntervalSeconds <= 0 || config.TimeoutSeconds <= 0 ||
		config.ProcessorName == "" || config.ProcessorName != strings.TrimSpace(config.ProcessorName) ||
		!utf8.ValidString(config.ProcessorName) || strings.ContainsRune(config.ProcessorName, 0) || utf8.RuneCountInString(config.ProcessorName) > 128 ||
		!utf8.ValidString(config.Params) || strings.ContainsRune(config.Params, 0) {
		return nil, errors.New("registered dispatch input is invalid")
	}
	deadline := timestamppb.New(attempt.DeadlineAt)
	if attempt.DeadlineAt.IsZero() || deadline.CheckValid() != nil {
		return nil, errors.New("registered dispatch deadline is invalid")
	}
	request := &protocol.DispatchTaskRequest{
		JobId: attempt.JobID, InstanceId: attempt.InstanceID, TaskId: attempt.TaskID, AttemptNo: attempt.AttemptNo,
		ProcessorName: config.ProcessorName, Params: config.Params, DeadlineAt: deadline,
		RequiredPost: config.PostEnabled,
	}
	switch attempt.TaskType {
	case model.TaskTypeOrdinary:
		if config.ExecutionMode != model.ExecutionModeNormal || attempt.InputData != nil {
			return nil, errors.New("registered ordinary input does not match its configuration")
		}
		request.Input = &protocol.DispatchTaskRequest_Ordinary{Ordinary: &protocol.OrdinaryInput{}}
	case model.TaskTypePrepare:
		if config.ExecutionMode != model.ExecutionModeSharded || attempt.InputData != nil {
			return nil, errors.New("registered preparation input does not match its configuration")
		}
		request.Input = &protocol.DispatchTaskRequest_Prepare{Prepare: &protocol.PrepareInput{}}
	case model.TaskTypeShard:
		if config.ExecutionMode != model.ExecutionModeSharded || !json.Valid(attempt.InputData) {
			return nil, errors.New("registered shard input is invalid")
		}
		request.Input = &protocol.DispatchTaskRequest_Shard{Shard: &protocol.ShardInput{Item: append([]byte(nil), attempt.InputData...)}}
	case model.TaskTypePost:
		if config.ExecutionMode != model.ExecutionModeSharded || !config.PostEnabled {
			return nil, errors.New("registered Post input does not match its configuration")
		}
		post, err := decodePostInput(attempt.InputData)
		if err != nil {
			return nil, err
		}
		request.Input = &protocol.DispatchTaskRequest_Post{Post: post}
	default:
		return nil, errors.New("registered dispatch Task type is invalid")
	}
	return request, nil
}

// decodePostInput requires all frozen counts. Missing fields and JSON null must
// not silently become zero; subtracting validated counts avoids sum overflow.
func decodePostInput(data model.TaskData) (*protocol.PostInput, error) {
	var counts struct {
		Total     *int64 `json:"total"`
		Succeeded *int64 `json:"succeeded"`
		Failed    *int64 `json:"failed"`
	}
	if err := json.Unmarshal(data, &counts); err != nil || counts.Total == nil || counts.Succeeded == nil || counts.Failed == nil {
		return nil, errors.New("registered Post statistics are missing or invalid")
	}
	total, succeeded, failed := *counts.Total, *counts.Succeeded, *counts.Failed
	if total < 0 || succeeded < 0 || failed < 0 || succeeded > total || total-succeeded != failed {
		return nil, errors.New("registered Post statistics are inconsistent")
	}
	return &protocol.PostInput{Total: total, Succeeded: succeeded, Failed: failed}, nil
}

func dispatchRejectionSummary(err error) string {
	st := status.Convert(err)
	switch st.Code() {
	case codes.Unauthenticated:
		return "executor authentication rejected"
	case codes.PermissionDenied:
		return "executor group access denied"
	case codes.Code(bizerror.DefaultCode):
		if reason, ok := bizerror.SubcodeFromStatus(st); ok {
			switch reason {
			case subcode.InvalidRequest:
				return "executor rejected invalid dispatch input"
			case protocol.DispatchRejected:
				return "executor rejected dispatch before admission"
			}
		}
	}
	return ""
}

// dispatchPeerAddress accepts only the actual TCP peer supplied by this call.
// It cannot detect deployment proxies; operators must ensure the peer identifies
// the selected executor instance rather than a shared load-balancer address.
func dispatchPeerAddress(address net.Addr) (string, bool) {
	tcp, ok := address.(*net.TCPAddr)
	if !ok || tcp == nil || tcp.IP.To16() == nil || tcp.IP.IsUnspecified() || tcp.IP.IsMulticast() || tcp.Port <= 0 || tcp.Port > 65535 {
		return "", false
	}
	return tcp.String(), true
}

func nilExecutorDependency(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
