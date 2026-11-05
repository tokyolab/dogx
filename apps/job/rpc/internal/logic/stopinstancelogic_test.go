package logic

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	jobsubcode "github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStopInstanceRejectsInvalidInputBeforeRequestingStop(t *testing.T) {
	for _, in := range []*job.StopInstanceRequest{nil, {}, {Id: -1}} {
		response, err := NewStopInstanceLogic(context.Background(), &svc.ServiceContext{}).StopInstance(in)
		if response != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid input reached the stop controller")
		}
	}
}

func TestStopInstanceReturnsObservedStateAndOnlyNotifiesNewDecision(t *testing.T) {
	manual, timeout, failed, empty := model.EndReasonManualStop, model.EndReasonInstanceTimeout, "task_failed", ""
	for _, test := range []struct {
		name   string
		state  model.InstanceStatus
		reason *string
		new    bool
	}{
		{"new manual stop", model.InstanceStopping, &manual, true},
		{"already stopping manually", model.InstanceStopping, &manual, false},
		{"already stopping on timeout", model.InstanceStopping, &timeout, false},
		{"already succeeded", model.InstanceSucceeded, nil, false},
		{"already failed", model.InstanceFailed, &failed, false},
		{"already canceled", model.InstanceCanceled, &empty, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			stub := &instanceStopperStub{decision: instancestop.Decision{
				InstanceID: 9, ExecutorGroupID: 3, Status: test.state, EndReason: test.reason, New: test.new,
			}}
			var notifications []instancestop.Decision
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &svc.ServiceContext{InstanceStopper: stub, AfterInstanceStopped: func(decision instancestop.Decision) {
				if !stub.returned {
					t.Fatal("stop hint ran before the controller confirmed commit")
				}
				notifications = append(notifications, decision)
			}}
			response, err := NewStopInstanceLogic(ctx, s).StopInstance(&job.StopInstanceRequest{Id: 9})
			if err != nil || response == nil || response.InstanceId != 9 || response.Status != int32(test.state) || response.EndReason != test.reason ||
				stub.calls != 1 || stub.id != 9 || stub.ctx != ctx {
				t.Fatalf("stop facts or context changed: response=%+v error=%v", response, err)
			}
			if test.new {
				if len(notifications) != 1 || notifications[0] != stub.decision {
					t.Fatal("newly committed stop was not handed off exactly once")
				}
			} else if len(notifications) != 0 {
				t.Fatal("an existing stop or terminal state emitted another stop hint")
			}
		})
	}
}

func TestStopInstanceErrorsNeverPublishSuccessOrNotify(t *testing.T) {
	for _, cause := range []error{
		fmt.Errorf("missing parent: %w", repository.ErrInstanceNotFound),
		errors.New("synthetic commit confirmation lost"),
		context.DeadlineExceeded,
		context.Canceled,
	} {
		stub := &instanceStopperStub{decision: instancestop.Decision{InstanceID: 9, Status: model.InstanceStopping, New: true}, err: cause}
		s := &svc.ServiceContext{InstanceStopper: stub, AfterInstanceStopped: func(instancestop.Decision) {
			t.Fatal("a failed stop emitted a post-commit hint")
		}}
		response, err := NewStopInstanceLogic(context.Background(), s).StopInstance(&job.StopInstanceRequest{Id: 9})
		if response != nil || err == nil {
			t.Fatal("unconfirmed stop was reported as success")
		}
		if errors.Is(cause, repository.ErrInstanceNotFound) {
			requireSubcode(t, err, jobsubcode.InstanceNotFound)
		} else if !errors.Is(err, cause) {
			t.Fatal("stop failure lost its technical cause")
		}
		if errors.Is(cause, context.DeadlineExceeded) && status.Code(err) != codes.DeadlineExceeded {
			t.Fatal("stop deadline was not preserved")
		}
		if errors.Is(cause, context.Canceled) && status.Code(err) != codes.Canceled {
			t.Fatal("stop context cancellation was not preserved")
		}
	}
}

func TestStopInstanceNotificationCannotUndoCommittedSuccess(t *testing.T) {
	for _, notify := range []func(instancestop.Decision){nil, func(instancestop.Decision) { panic("synthetic notification panic") }} {
		reason := model.EndReasonManualStop
		stub := &instanceStopperStub{decision: instancestop.Decision{InstanceID: 9, Status: model.InstanceStopping, EndReason: &reason, New: true}}
		s := &svc.ServiceContext{InstanceStopper: stub, AfterInstanceStopped: notify}
		response, err := NewStopInstanceLogic(context.Background(), s).StopInstance(&job.StopInstanceRequest{Id: 9})
		if err != nil || response == nil || response.InstanceId != 9 || response.Status != int32(model.InstanceStopping) || response.GetEndReason() != reason {
			t.Fatal("notification failure changed the committed response")
		}
	}
}

type instanceStopperStub struct {
	decision instancestop.Decision
	err      error
	ctx      context.Context
	id       int64
	calls    int
	returned bool
}

func (s *instanceStopperStub) RequestManual(ctx context.Context, id int64) (instancestop.Decision, error) {
	s.ctx, s.id, s.calls, s.returned = ctx, id, s.calls+1, true
	return s.decision, s.err
}
