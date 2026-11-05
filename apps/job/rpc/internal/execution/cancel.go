package execution

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"time"
	"unicode"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	cancelRPCTimeout    = time.Second
	cancelRetryInterval = time.Second
	cancelTargetTimeout = 5 * time.Second
	cancelMaxAttempts   = 3
)

// CancelTarget names one persisted executor address for a confirmed Instance stop
// decision. Reason must be manual_stop or instance_timeout from that decision,
// never inferred from a caller or RPC deadline. Address is the saved TCP peer,
// not a discovery key. The caller must not infer committed stopping from an error.
type CancelTarget struct {
	InstanceID      int64
	ExecutorGroupID int64
	Address         string
	Reason          string
}

// cancelGroups resolves current immutable credentials from the executor-group
// cache. Implementations must support concurrent calls and keep the lookup bound
// to ctx. Missing or deleted groups return an error, never an empty success.
type cancelGroups interface {
	ByID(context.Context, int64) (repository.ExecutorGroupConfig, error)
}

// cancelClient sends one Instance notification. A response only acknowledges the
// local cancellation signal; it does not prove that business execution exited.
type cancelClient interface {
	CancelInstance(context.Context, *protocol.CancelInstanceRequest, ...grpc.CallOption) (*protocol.CancelInstanceResponse, error)
}

// cancelConnection is owned by one Cancel call, including all of its retries.
// The factory must release resources it does not return, including on failure.
// Cancel closes any returned connection with a close function, even on error.
// A successful connection supplies both fields and remains usable until closed.
type cancelConnection struct {
	client cancelClient
	close  func() error
}

// cancelConnectionFactory creates synchronously: zrpc has no construction Context.
// Nonblocking direct dialing avoids discovery, but the caller still waits for
// actual return before checking its deadline and releasing this connection.
type cancelConnectionFactory func(zrpc.RpcClientConf, time.Duration) (*cancelConnection, error)

// Canceller sends best-effort notifications to one saved address per call. It
// owns no queue, discovery client cache, background worker or database transition.
// Concurrent calls are independent if the group cache is concurrency-safe. Each
// call owns one temporary direct connection and releases it after actual RPC exit.
type Canceller struct {
	groups cancelGroups
	config zrpc.RpcClientConf
	create cancelConnectionFactory
}

// NewCanceller binds the existing group cache without looking up credentials or
// opening a connection. Cancellation uses at most three one-second RPC attempts,
// separated by one second, within a five-second target budget. Earlier caller
// deadlines also apply; the caller supplies its Instance and shutdown budgets.
func NewCanceller(groups cancelGroups) (*Canceller, error) {
	if nilExecutorDependency(groups) {
		return nil, errors.New("executor cancellation group cache must not be nil")
	}
	var config zrpc.RpcClientConf
	if err := conf.FillDefault(&config); err != nil {
		return nil, &cancelError{operation: "initialize cancellation client configuration", cause: err}
	}
	config.NonBlock = true
	return &Canceller{groups: groups, config: config, create: createCancelConnection}, nil
}

func createCancelConnection(config zrpc.RpcClientConf, timeout time.Duration) (*cancelConnection, error) {
	client, err := NewExecutorClient(config, timeout)
	if err != nil {
		return nil, err
	}
	return &cancelConnection{client: protocol.NewExecutorClient(client.Conn()), close: client.Conn().Close}, nil
}

// Cancel notifies only target.Address and never checks or changes persisted state.
// The caller must provide an already-confirmed stop decision and a control Context
// independent of an expired business deadline. Group lookup, construction, calls
// and retry waits share one target budget. A valid response ends notification even
// if no local activity matched; nil does not imply business exit or database state.
//
// Only Unauthenticated, PermissionDenied and the default business code carrying
// common.invalid_request are permanent RPC refusals. Other errors and a missing
// acknowledgement remain unconfirmed and permit another bounded attempt on the
// same connection. Local lookup, validation and construction failures are not retried.
// No call is detached from its caller: cancellation requests stop, but this method
// waits for dependencies and connection cleanup to actually return. It therefore
// does not promise hard resource release at the deadline. Errors retain causes
// for errors.Is/As while their own text excludes upstream or credential content.
func (c *Canceller) Cancel(ctx context.Context, target CancelTarget) (result error) {
	if ctx == nil {
		return errors.New("executor cancellation context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	reason, err := cancelTargetReason(target)
	if err != nil {
		return err
	}
	workCtx, cancel := context.WithTimeout(ctx, cancelTargetTimeout)
	defer cancel()
	group, err := c.groups.ByID(workCtx, target.ExecutorGroupID)
	if err != nil {
		return &cancelError{operation: "resolve cancellation group", cause: err}
	}
	if group.ID != target.ExecutorGroupID {
		return errors.New("executor cancellation group does not match the stopped instance")
	}
	if err := jobexecutor.ValidateCredentials(group.Name, group.Token); err != nil {
		return &cancelError{operation: "validate cancellation credentials", cause: err}
	}
	if err := workCtx.Err(); err != nil {
		return err
	}
	config := c.config
	config.Endpoints = []string{target.Address}
	connection, err := c.create(config, cancelRPCTimeout)
	if connection != nil && connection.close != nil {
		defer func() {
			if err := connection.close(); err != nil {
				result = errors.Join(result, &cancelError{operation: "close cancellation connection", cause: err})
			}
		}()
	}
	if err != nil {
		return &cancelError{operation: "create cancellation connection", cause: err}
	}
	if connection == nil || nilExecutorDependency(connection.client) || connection.close == nil {
		return errors.New("executor cancellation connection is incomplete")
	}
	request := &protocol.CancelInstanceRequest{Auth: &protocol.GroupAuth{GroupName: group.Name, Token: group.Token},
		InstanceId: target.InstanceID, Reason: reason}
	for attempt := 0; attempt < cancelMaxAttempts; attempt++ {
		if err := workCtx.Err(); err != nil {
			return err
		}
		rpcCtx, cancelRPC := context.WithTimeout(workCtx, cancelRPCTimeout)
		ack, err := connection.client.CancelInstance(rpcCtx, request)
		cancelRPC()
		if err == nil && ack != nil {
			return nil
		}
		if err == nil {
			// A normal empty protobuf response is non-nil. A missing response
			// provides no confirmation, so it follows the same finite retry path.
			err = errors.New("executor returned no cancellation acknowledgement")
		}
		if stopErr := workCtx.Err(); stopErr != nil {
			return &cancelError{operation: "cancel executor instance", cause: errors.Join(stopErr, err)}
		}
		if !retryCancel(err) || attempt == cancelMaxAttempts-1 {
			return &cancelError{operation: "cancel executor instance", cause: err}
		}
		timer := time.NewTimer(cancelRetryInterval)
		select {
		case <-workCtx.Done():
			timer.Stop()
			return workCtx.Err()
		case <-timer.C:
		}
	}
	return errors.New("executor cancellation attempts exhausted")
}

func cancelTargetReason(target CancelTarget) (protocol.CancelReason, error) {
	address, err := netip.ParseAddrPort(target.Address)
	if target.InstanceID <= 0 || target.ExecutorGroupID <= 0 || err != nil || address.Port() == 0 ||
		address.Addr().IsUnspecified() || address.Addr().IsMulticast() || len(target.Address) > 255 ||
		strings.ContainsAny(target.Address, "/\\?#@") || strings.IndexFunc(target.Address, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return 0, errors.New("executor cancellation target is invalid")
	}
	switch target.Reason {
	case model.EndReasonManualStop:
		return protocol.CancelReason_MANUAL_STOP, nil
	case model.EndReasonInstanceTimeout:
		return protocol.CancelReason_INSTANCE_TIMEOUT, nil
	default:
		return 0, errors.New("executor cancellation reason is invalid")
	}
}

func retryCancel(err error) bool {
	if err == nil {
		return false
	}
	st := status.Convert(err)
	switch st.Code() {
	case codes.Unauthenticated, codes.PermissionDenied:
		return false
	case codes.Code(bizerror.DefaultCode):
		if reason, ok := bizerror.SubcodeFromStatus(st); ok && reason == subcode.InvalidRequest {
			return false
		}
	}
	return true
}

// cancelError keeps diagnostic causes inspectable without formatting server
// messages, RPC bodies or group credentials at the cancellation boundary.
type cancelError struct {
	operation string
	cause     error
}

func (e *cancelError) Error() string { return e.operation + " failed" }
func (e *cancelError) Unwrap() error { return e.cause }
