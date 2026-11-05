package execution

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCancellerConstructionAndInvalidInputDoNotOpenConnections(t *testing.T) {
	var absent *cancelGroupsStub
	for _, groups := range []cancelGroups{nil, absent} {
		if component, err := NewCanceller(groups); component != nil || err == nil {
			t.Fatal("nil group source accepted")
		}
	}
	for _, edit := range []func(*CancelTarget){
		func(target *CancelTarget) { target.InstanceID = 0 },
		func(target *CancelTarget) { target.ExecutorGroupID = -1 },
		func(target *CancelTarget) { target.Reason = "success" },
		func(target *CancelTarget) { target.Address = "" },
		func(target *CancelTarget) { target.Address = "127.0.0.1:0" },
		func(target *CancelTarget) { target.Address = "0.0.0.0:9000" },
		func(target *CancelTarget) { target.Address = "[::]:9000" },
		func(target *CancelTarget) { target.Address = "224.0.0.1:9000" },
		func(target *CancelTarget) { target.Address = "discovery.invalid:9000" },
		func(target *CancelTarget) { target.Address = "127.0.0.1:9000\n" },
		func(target *CancelTarget) { target.Address = "http://127.0.0.1:9000" },
	} {
		groups, client := cancelGroupFixture(), &cancelClientStub{}
		component, factories, closed := newTestCanceller(t, groups, client)
		target := cancelTargetFixture()
		edit(&target)
		if err := component.Cancel(context.Background(), target); err == nil || groups.calls != 0 || *factories != 0 || *closed != 0 {
			t.Fatal("invalid cancellation input performed lookup or opened a connection")
		}
	}
	groups := cancelGroupFixture()
	component, factories, _ := newTestCanceller(t, groups, &cancelClientStub{})
	if err := component.Cancel(nil, cancelTargetFixture()); err == nil {
		t.Fatal("nil cancellation context accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := component.Cancel(ctx, cancelTargetFixture()); !errors.Is(err, context.Canceled) || groups.calls != 0 || *factories != 0 {
		t.Fatal("canceled input resolved a group or opened a connection")
	}
}

func TestCancellerMapsStopDecisionAndUsesOneTemporaryClient(t *testing.T) {
	for _, scenario := range []struct {
		reason  string
		wire    protocol.CancelReason
		address string
	}{
		{model.EndReasonManualStop, protocol.CancelReason_MANUAL_STOP, "127.0.0.1:9010"},
		{model.EndReasonInstanceTimeout, protocol.CancelReason_INSTANCE_TIMEOUT, "[::1]:9010"},
	} {
		t.Run(scenario.reason, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &cancelClientStub{call: func(_ context.Context, index int) (*protocol.CancelInstanceResponse, error) {
					if index < 3 {
						return nil, status.Error(codes.Unavailable, "synthetic-private-upstream-diagnostic")
					}
					return &protocol.CancelInstanceResponse{}, nil
				}}
				groups := cancelGroupFixture()
				component, factories, closed := newTestCanceller(t, groups, client)
				target := cancelTargetFixture()
				target.Reason, target.Address = scenario.reason, scenario.address
				started := time.Now()
				if err := component.Cancel(context.Background(), target); err != nil {
					t.Fatal(err)
				}
				if *factories != 1 || *closed != 1 || groups.calls != 1 || groups.id != target.ExecutorGroupID || len(client.requests) != 3 || time.Since(started) != 2*time.Second {
					t.Fatal("bounded retries replaced the target connection, credentials or retry timing")
				}
				for index, request := range client.requests {
					if request.InstanceId != target.InstanceID || request.Reason != scenario.wire || request.Auth.GetGroupName() != groups.group.Name || request.Auth.GetToken() != groups.group.Token ||
						client.budgets[index] != time.Second || client.options[index] != 0 || !client.started[index].Equal(started.Add(time.Duration(index)*time.Second)) {
						t.Fatal("cancellation changed its Instance, reason, credentials, per-call budget or call options")
					}
				}
				if err := component.Cancel(context.Background(), target); err != nil || *factories != 2 || *closed != 2 {
					t.Fatal("separate notifications reused a temporary connection")
				}
			})
		})
	}
}

func TestCancellerRetriesUnconfirmedErrorsExceptDocumentedPermanentRefusals(t *testing.T) {
	private := "synthetic-private-cancel-diagnostic"
	for _, scenario := range []struct {
		name string
		err  error
		want int
	}{
		{"unavailable", status.Error(codes.Unavailable, private), 3},
		{"RPC deadline", status.Error(codes.DeadlineExceeded, private), 3},
		{"context deadline", context.DeadlineExceeded, 3},
		{"authentication", status.Error(codes.Unauthenticated, private), 1},
		{"group", status.Error(codes.PermissionDenied, private), 1},
		{"invalid request", senderStatus(t, codes.Code(bizerror.DefaultCode), subcode.InvalidRequest), 1},
		{"wrapped authentication", fmt.Errorf("%s: %w", private, status.Error(codes.Unauthenticated, private)), 1},
		{"wrapped invalid request", fmt.Errorf("%s: %w", private, senderStatus(t, codes.Code(bizerror.DefaultCode), subcode.InvalidRequest)), 1},
		{"standard argument code", status.Error(codes.InvalidArgument, private), 3},
		{"standard argument with detail", senderStatus(t, codes.InvalidArgument, subcode.InvalidRequest), 3},
		{"unrecognized business code", senderStatus(t, codes.Code(bizerror.DefaultCode+1), subcode.InvalidRequest), 3},
		{"unrecognized subcode", senderStatus(t, codes.Code(bizerror.DefaultCode), subcode.ResourceNotFound), 3},
		{"dispatch refusal is not cancel refusal", senderStatus(t, codes.Code(bizerror.DefaultCode), protocol.DispatchRejected), 3},
		{"message without detail", status.Error(codes.Code(bizerror.DefaultCode), subcode.InvalidRequest), 3},
		{"missing group code", status.Error(codes.NotFound, private), 3},
		{"remote cancellation", status.Error(codes.Canceled, private), 3},
		{"dependency cancellation with live caller", context.Canceled, 3},
		{"overloaded", status.Error(codes.ResourceExhausted, private), 3},
		{"internal", status.Error(codes.Internal, private), 3},
		{"unknown", errors.New(private), 3},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &cancelClientStub{call: func(context.Context, int) (*protocol.CancelInstanceResponse, error) { return nil, scenario.err }}
				component, factories, closed := newTestCanceller(t, cancelGroupFixture(), client)
				err := component.Cancel(context.Background(), cancelTargetFixture())
				if err == nil || !errors.Is(err, scenario.err) || strings.Contains(err.Error(), private) || len(client.requests) != scenario.want || *factories != 1 || *closed != 1 {
					t.Fatal("permanent refusal classification, safe error cause or connection cleanup was incorrect")
				}
			})
		})
	}

}

func TestCancellerMissingAcknowledgementUsesOriginalRetryBudget(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		ackOn      int
		caller     time.Duration
		wantCalls  int
		wantTime   time.Duration
		wantFailed bool
	}{
		{"always missing", 0, 0, 3, 2 * time.Second, true},
		{"later confirmation", 2, 0, 2, time.Second, false},
		{"caller expires during retry wait", 0, 250 * time.Millisecond, 1, 250 * time.Millisecond, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &cancelClientStub{call: func(_ context.Context, index int) (*protocol.CancelInstanceResponse, error) {
					if index == scenario.ackOn {
						return &protocol.CancelInstanceResponse{}, nil
					}
					return nil, nil
				}}
				component, factories, closed := newTestCanceller(t, cancelGroupFixture(), client)
				ctx := context.Background()
				if scenario.caller > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, scenario.caller)
					defer cancel()
				}
				started := time.Now()
				err := component.Cancel(ctx, cancelTargetFixture())
				if (err != nil) != scenario.wantFailed || len(client.requests) != scenario.wantCalls || time.Since(started) != scenario.wantTime || *factories != 1 || *closed != 1 {
					t.Fatal("missing acknowledgement ended as success or changed retry, timing or connection limits")
				}
				if scenario.caller > 0 && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("unconfirmed response lost the earlier caller deadline")
				}
			})
		})
	}
}

func TestCancellerTargetAndCallerBudgetsIncludeLookupAndCreation(t *testing.T) {
	for _, scenario := range []struct {
		name           string
		lookup, create time.Duration
		caller         time.Duration
		wantBudgets    []time.Duration
		wantCreate     int
		wantElapsed    time.Duration
	}{
		{"three timed out calls", 0, 0, 0, []time.Duration{time.Second, time.Second, time.Second}, 1, 5 * time.Second},
		{"lookup consumes target", 2500 * time.Millisecond, 0, 0, []time.Duration{time.Second, 500 * time.Millisecond}, 1, 5 * time.Second},
		{"creation consumes target", 0, 2500 * time.Millisecond, 0, []time.Duration{time.Second, 500 * time.Millisecond}, 1, 5 * time.Second},
		{"earlier caller deadline", 0, 0, 250 * time.Millisecond, []time.Duration{250 * time.Millisecond}, 1, 250 * time.Millisecond},
		{"lookup outlives target", 6 * time.Second, 0, 0, nil, 0, 6 * time.Second},
		{"creation outlives target", 0, 6 * time.Second, 0, nil, 1, 6 * time.Second},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				groups := cancelGroupFixture()
				groups.delay = scenario.lookup
				client := &cancelClientStub{call: func(ctx context.Context, _ int) (*protocol.CancelInstanceResponse, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				}}
				component, factories, closed := newTestCanceller(t, groups, client)
				factory := component.create
				component.create = func(config zrpc.RpcClientConf, timeout time.Duration) (*cancelConnection, error) {
					time.Sleep(scenario.create)
					return factory(config, timeout)
				}
				ctx := context.Background()
				if scenario.caller > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, scenario.caller)
					defer cancel()
				}
				started := time.Now()
				err := component.Cancel(ctx, cancelTargetFixture())
				if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(client.budgets, scenario.wantBudgets) || *factories != scenario.wantCreate || *closed != scenario.wantCreate || time.Since(started) != scenario.wantElapsed {
					t.Fatalf("budget boundary: budgets=%v creates=%d closes=%d elapsed=%v", client.budgets, *factories, *closed, time.Since(started))
				}
			})
		})
	}
}

func TestCancellerCancellationStopsRetryButWaitsForActualRPCAndClose(t *testing.T) {
	for _, blockedRPC := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry wait", true: "in-flight RPC"}[blockedRPC], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release, closeRelease := make(chan struct{}), make(chan struct{})
				client := &cancelClientStub{call: func(ctx context.Context, _ int) (*protocol.CancelInstanceResponse, error) {
					if blockedRPC {
						<-release
						return nil, ctx.Err()
					}
					return nil, status.Error(codes.Unavailable, "temporary communication failure")
				}}
				component, factories, closed := newTestCanceller(t, cancelGroupFixture(), client)
				factory := component.create
				component.create = func(config zrpc.RpcClientConf, timeout time.Duration) (*cancelConnection, error) {
					connection, err := factory(config, timeout)
					closeConnection := connection.close
					connection.close = func() error { <-closeRelease; return closeConnection() }
					return connection, err
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				finished := make(chan error, 1)
				go func() { finished <- component.Cancel(ctx, cancelTargetFixture()) }()
				synctest.Wait()
				cancel()
				synctest.Wait()
				if *factories != 1 || *closed != 0 || len(client.requests) != 1 {
					t.Fatal("cancellation detached or replaced active work")
				}
				close(release)
				synctest.Wait()
				select {
				case <-finished:
					t.Fatal("Cancel returned before connection cleanup actually exited")
				default:
				}
				close(closeRelease)
				if err := <-finished; !errors.Is(err, context.Canceled) || *closed != 1 || len(client.requests) != 1 {
					t.Fatal("canceled notification retried or failed to close its connection")
				}
			})
		})
	}
}

func TestCancellerGroupAndConnectionFailuresAreSafeAndNotRetried(t *testing.T) {
	private := errors.New("synthetic-private-cancellation-credentials")
	for _, edit := range []func(*cancelGroupsStub){
		func(groups *cancelGroupsStub) { groups.err = private },
		func(groups *cancelGroupsStub) { groups.group.ID++ },
		func(groups *cancelGroupsStub) { groups.group.Name = " " },
		func(groups *cancelGroupsStub) { groups.group.Token = private.Error() },
	} {
		groups := cancelGroupFixture()
		edit(groups)
		component, factories, _ := newTestCanceller(t, groups, &cancelClientStub{})
		err := component.Cancel(context.Background(), cancelTargetFixture())
		if err == nil || strings.Contains(err.Error(), private.Error()) || groups.calls != 1 || *factories != 0 {
			t.Fatal("group lookup or validation failure retried, disclosed credentials or opened a connection")
		}
	}
	for _, failure := range []string{"construction", "missing client", "close"} {
		t.Run(failure, func(t *testing.T) {
			component, factories, closed := newTestCanceller(t, cancelGroupFixture(), &cancelClientStub{})
			factory := component.create
			component.create = func(config zrpc.RpcClientConf, timeout time.Duration) (*cancelConnection, error) {
				connection, err := factory(config, timeout)
				switch failure {
				case "construction":
					return connection, private
				case "missing client":
					var absent *cancelClientStub
					connection.client = absent
				case "close":
					closeConnection := connection.close
					connection.close = func() error { _ = closeConnection(); return private }
				}
				return connection, err
			}
			err := component.Cancel(context.Background(), cancelTargetFixture())
			if err == nil || strings.Contains(err.Error(), private.Error()) || *factories != 1 || *closed != 1 ||
				(failure != "missing client" && !errors.Is(err, private)) {
				t.Fatal("temporary client failure lost ownership or disclosed its diagnostic")
			}
		})
	}
}

type cancelGroupsStub struct {
	group repository.ExecutorGroupConfig
	err   error
	delay time.Duration
	calls int
	id    int64
}

func (s *cancelGroupsStub) ByID(_ context.Context, id int64) (repository.ExecutorGroupConfig, error) {
	s.calls++
	s.id = id
	time.Sleep(s.delay)
	return s.group, s.err
}

type cancelClientStub struct {
	call     func(context.Context, int) (*protocol.CancelInstanceResponse, error)
	requests []*protocol.CancelInstanceRequest
	budgets  []time.Duration
	started  []time.Time
	options  []int
}

func (s *cancelClientStub) CancelInstance(ctx context.Context, request *protocol.CancelInstanceRequest, options ...grpc.CallOption) (*protocol.CancelInstanceResponse, error) {
	s.requests = append(s.requests, proto.Clone(request).(*protocol.CancelInstanceRequest))
	deadline, _ := ctx.Deadline()
	s.budgets = append(s.budgets, time.Until(deadline))
	s.started = append(s.started, time.Now())
	s.options = append(s.options, len(options))
	if s.call != nil {
		return s.call(ctx, len(s.requests))
	}
	return &protocol.CancelInstanceResponse{}, nil
}

func cancelGroupFixture() *cancelGroupsStub {
	return &cancelGroupsStub{group: repository.ExecutorGroupConfig{ID: 11, Name: "test-workers", Token: receiverTestToken, DiscoveryKey: "/unused/executor/discovery"}}
}

func cancelTargetFixture() CancelTarget {
	return CancelTarget{InstanceID: 22, ExecutorGroupID: 11, Address: "127.0.0.1:9010", Reason: model.EndReasonManualStop}
}

func newTestCanceller(t *testing.T, groups *cancelGroupsStub, client cancelClient) (*Canceller, *int, *int) {
	t.Helper()
	component, err := NewCanceller(groups)
	if err != nil {
		t.Fatal(err)
	}
	created, closed := 0, 0
	component.create = func(config zrpc.RpcClientConf, timeout time.Duration) (*cancelConnection, error) {
		created++
		if len(config.Endpoints) != 1 || len(config.Etcd.Hosts) != 0 || config.Etcd.Key != "" || config.Target != "" ||
			config.App != "" || config.Token != "" || !config.NonBlock || timeout != time.Second {
			t.Fatal("cancellation did not create one direct, independently timed client without native credentials")
		}
		return &cancelConnection{client: client, close: func() error { closed++; return nil }}, nil
	}
	return component, &created, &closed
}
