// Package execution adapts the scheduler's execution communication RPCs to
// native runtime contracts without exposing transport credentials downstream.
package execution

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrInvalidCredentials requires a definite authentication failure, not a
	// cache miss or failed database lookup.
	ErrInvalidCredentials = errors.New("result reporter credentials are invalid")
	// ErrGroupDenied means the authenticated group is not permitted access.
	ErrGroupDenied = errors.New("result reporter group is not permitted")
)

// GroupAuthenticator resolves request credentials to a trusted group identity.
// Implementations must support concurrent calls, merge cache misses as needed,
// and distinguish definite credential or access failures from technical lookup
// errors. Authentication must not query Task rows or log the supplied token.
type GroupAuthenticator interface {
	// Authenticate returns a positive group ID only after successful validation.
	// ErrInvalidCredentials and ErrGroupDenied are definite failures; all other
	// errors represent temporary inability to establish the caller's identity.
	// A cache miss alone is not a definite failure. The Context bounds lookup.
	Authenticate(ctx context.Context, groupName, token string) (int64, error)
}

// ResultAcceptor owns bounded result admission and the later persistence path.
// It must support concurrent calls and may retain an accepted report beyond
// the RPC lifetime. Its error vocabulary is independent of gRPC and bizerror.
type ResultAcceptor interface {
	// Accept returns (true, nil) only after the report has entered the bounded
	// local buffer, not after database commit. (false, nil) guarantees refusal
	// because the buffer is full. taskresult.ErrNotReady indicates unavailable
	// admission, including queue startup and shutdown.
	// An error must never be converted into an ACK. The Context bounds admission
	// only; cancellation does not prove that previously accepted data was lost.
	Accept(ctx context.Context, report taskresult.Report) (bool, error)
}

// Receiver authenticates and validates one result before local admission. It
// does not query Task state, verify the persisted stage or wait for persistence.
// Register it with bizerror.UnaryServerInterceptor and disable body logging.
type Receiver struct {
	protocol.UnimplementedResultReceiverServer
	auth    GroupAuthenticator
	results ResultAcceptor
}

// NewReceiver requires both runtime dependencies, including non-nil concrete
// values. It does not install a fallback acceptor or start background work.
func NewReceiver(auth GroupAuthenticator, results ResultAcceptor) (*Receiver, error) {
	if nilReceiverDependency(auth) {
		return nil, errors.New("result authenticator must not be nil")
	}
	if nilReceiverDependency(results) {
		return nil, errors.New("result acceptor must not be nil")
	}
	return &Receiver{auth: auth, results: results}, nil
}

// ReportTaskResult returns an ACK only after local result admission. Permanent
// wire errors use common.invalid_request; full buffers and unavailable identity
// lookup remain retryable. Neither an ACK nor a failure proves a database outcome.
func (r *Receiver) ReportTaskResult(ctx context.Context, req *protocol.ReportTaskResultRequest) (*protocol.ReportTaskResultResponse, error) {
	auth := req.GetAuth()
	if auth.GetGroupName() == "" || auth.GetToken() == "" {
		return nil, status.Error(codes.Unauthenticated, "result reporter credentials are required")
	}
	groupID, err := r.auth.Authenticate(ctx, auth.GroupName, auth.Token)
	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidCredentials):
			return nil, status.Error(codes.Unauthenticated, "result reporter credentials are invalid")
		case errors.Is(err, ErrGroupDenied):
			return nil, status.Error(codes.PermissionDenied, "result reporter group is not permitted")
		default:
			return nil, status.Error(codes.Unavailable, "result reporter authentication is unavailable")
		}
	}
	if groupID <= 0 {
		return nil, status.Error(codes.Unavailable, "result reporter authentication is unavailable")
	}
	report, err := parseResult(req, groupID)
	if err != nil {
		return nil, err
	}
	accepted, err := r.results.Accept(ctx, report)
	if err != nil {
		switch {
		case errors.Is(err, taskresult.ErrNotReady):
			return nil, status.Error(codes.Unavailable, "result receiver is not ready")
		case errors.Is(err, context.Canceled):
			return nil, status.Error(codes.Canceled, "result admission was cancelled")
		case errors.Is(err, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, "result admission deadline was exceeded")
		default:
			return nil, status.Error(codes.Internal, "result admission failed")
		}
	}
	if !accepted {
		return nil, status.Error(codes.ResourceExhausted, "result buffer is full")
	}
	return &protocol.ReportTaskResultResponse{}, nil
}

func parseResult(req *protocol.ReportTaskResultRequest, groupID int64) (taskresult.Report, error) {
	invalid := func(message string) (taskresult.Report, error) {
		return taskresult.Report{}, bizerror.New(subcode.InvalidRequest, message)
	}
	if req.JobId <= 0 || req.InstanceId <= 0 || req.TaskId <= 0 || req.AttemptNo <= 0 {
		return invalid("result IDs and attempt number must be positive")
	}
	identity, err := uuid.Parse(req.ExecutorInstanceId)
	if err != nil || identity.Version() != 4 || identity.Variant() != uuid.RFC4122 || req.ExecutorInstanceId != identity.String() {
		return invalid("executor process identity is invalid")
	}
	report := taskresult.Report{
		GroupID: groupID, JobID: req.JobId, InstanceID: req.InstanceId,
		TaskID: req.TaskId, AttemptNo: req.AttemptNo, ExecutorInstanceID: req.ExecutorInstanceId,
		Summary: req.Summary,
	}
	switch req.Outcome {
	case protocol.TaskOutcome_SUCCESS:
		report.Outcome = taskresult.OutcomeSuccess
	case protocol.TaskOutcome_FAILURE:
		report.Outcome = taskresult.OutcomeFailure
		// Failed business work does not supply usable output. Rejecting unused
		// malformed Data here would discard an otherwise valid failure result.
		return report, nil
	default:
		return invalid("result outcome is invalid")
	}
	switch output := req.Output.(type) {
	case nil:
		return report, nil
	case *protocol.ReportTaskResultRequest_DataJson:
		if output == nil || !json.Valid(output.DataJson) {
			return invalid("result data must contain valid JSON")
		}
		report.Data = append(json.RawMessage(nil), output.DataJson...)
	case *protocol.ReportTaskResultRequest_PreparedItems:
		if output == nil || output.PreparedItems == nil {
			return invalid("prepared result items are required")
		}
		report.PreparedItems = make([]json.RawMessage, len(output.PreparedItems.Items))
		for index, item := range output.PreparedItems.Items {
			if !json.Valid(item) {
				return invalid("prepared result item must contain valid JSON")
			}
			report.PreparedItems[index] = append(json.RawMessage(nil), item...)
		}
	default:
		return invalid("result output is invalid")
	}
	return report, nil
}

func nilReceiverDependency(dependency any) bool {
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
