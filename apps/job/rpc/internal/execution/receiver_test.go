package execution

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/job/internal/taskresult"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/tokyolab/dogx/pkg/subcode"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These synthetic identities are isolated from deployed executor credentials.
const (
	receiverTestToken = "11111111111141118111111111111111"
	receiverTestID    = "22222222-2222-4222-8222-222222222222"
)

type receiverAuthStub struct {
	groupID    int64
	err        error
	calls      int
	groupNames []string
	tokens     []string
}

func (s *receiverAuthStub) Authenticate(_ context.Context, groupName, token string) (int64, error) {
	s.calls++
	s.groupNames = append(s.groupNames, groupName)
	s.tokens = append(s.tokens, token)
	return s.groupID, s.err
}

type receiverAcceptorStub struct {
	accepted bool
	err      error
	reports  []taskresult.Report
}

func (s *receiverAcceptorStub) Accept(_ context.Context, report taskresult.Report) (bool, error) {
	s.reports = append(s.reports, report)
	return s.accepted, s.err
}

func validReceiverRequest() *protocol.ReportTaskResultRequest {
	return &protocol.ReportTaskResultRequest{
		Auth:  &protocol.GroupAuth{GroupName: "test-workers", Token: receiverTestToken},
		JobId: 11, InstanceId: 22, TaskId: 33, AttemptNo: 4, ExecutorInstanceId: receiverTestID,
		Outcome: protocol.TaskOutcome_SUCCESS, Summary: "completed",
	}
}

func testReceiver(t *testing.T, auth GroupAuthenticator, acceptor ResultAcceptor) *Receiver {
	t.Helper()
	receiver, err := NewReceiver(auth, acceptor)
	if err != nil {
		t.Fatal(err)
	}
	return receiver
}

func TestReceiverAuthenticationAndAdmissionContract(t *testing.T) {
	for _, test := range []struct {
		name       string
		groupID    int64
		authErr    error
		accepted   bool
		acceptErr  error
		code       codes.Code
		shouldPass bool
	}{
		{name: "accepted", groupID: 41, accepted: true, shouldPass: true},
		{name: "buffer full", groupID: 41, code: codes.ResourceExhausted, shouldPass: true},
		{name: "wrong credentials", authErr: fmt.Errorf("lookup: %w", ErrInvalidCredentials), code: codes.Unauthenticated},
		{name: "group denied", authErr: ErrGroupDenied, code: codes.PermissionDenied},
		{name: "database unavailable", authErr: errors.New("database failure with " + receiverTestToken), code: codes.Unavailable},
		{name: "invalid authenticated identity", groupID: 0, code: codes.Unavailable},
		{name: "receiver not ready", groupID: 41, acceptErr: fmt.Errorf("accept: %w", taskresult.ErrNotReady), code: codes.Unavailable, shouldPass: true},
		{name: "accept outcome uncertain", groupID: 41, accepted: true, acceptErr: errors.New("unconfirmed admission"), code: codes.Internal, shouldPass: true},
		{name: "request cancelled", groupID: 41, acceptErr: context.Canceled, code: codes.Canceled, shouldPass: true},
		{name: "request deadline", groupID: 41, acceptErr: context.DeadlineExceeded, code: codes.DeadlineExceeded, shouldPass: true},
		{name: "wrapped request context", groupID: 41, acceptErr: fmt.Errorf("%s: %w", receiverTestToken, context.Canceled), code: codes.Canceled, shouldPass: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			auth := &receiverAuthStub{groupID: test.groupID, err: test.authErr}
			acceptor := &receiverAcceptorStub{accepted: test.accepted, err: test.acceptErr}
			receiver := testReceiver(t, auth, acceptor)
			response, err := receiver.ReportTaskResult(context.Background(), validReceiverRequest())
			if status.Code(err) != test.code {
				t.Fatalf("code = %v, want %v: %v", status.Code(err), test.code, err)
			}
			if err != nil && response != nil {
				t.Fatal("failed admission returned an ACK")
			}
			if err != nil && strings.Contains(err.Error(), receiverTestToken) {
				t.Fatal("reply leaked authentication data")
			}
			if test.shouldPass != (len(acceptor.reports) == 1) {
				t.Fatal("report crossed the wrong authentication boundary")
			}
			if test.shouldPass && acceptor.reports[0].GroupID != test.groupID {
				t.Fatal("acceptor did not receive the trusted group identity")
			}
		})
	}
}

func TestReceiverMissingCredentialsNeverReachRuntime(t *testing.T) {
	auth := &receiverAuthStub{groupID: 41}
	acceptor := &receiverAcceptorStub{accepted: true}
	receiver := testReceiver(t, auth, acceptor)
	requests := []*protocol.ReportTaskResultRequest{nil, {}, validReceiverRequest(), validReceiverRequest()}
	requests[2].Auth.GroupName = ""
	requests[3].Auth.Token = ""
	for _, req := range requests {
		if _, err := receiver.ReportTaskResult(context.Background(), req); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("missing credentials error = %v", err)
		}
	}
	if auth.calls != 0 || len(acceptor.reports) != 0 {
		t.Fatal("missing credentials were passed to a runtime dependency")
	}
}

func TestReceiverRejectsInvalidWireBeforeAdmission(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ReportTaskResultRequest)
	}{
		{"missing job ID", func(r *protocol.ReportTaskResultRequest) { r.JobId = 0 }},
		{"missing instance ID", func(r *protocol.ReportTaskResultRequest) { r.InstanceId = 0 }},
		{"missing task ID", func(r *protocol.ReportTaskResultRequest) { r.TaskId = 0 }},
		{"invalid attempt", func(r *protocol.ReportTaskResultRequest) { r.AttemptNo = -1 }},
		{"missing identity", func(r *protocol.ReportTaskResultRequest) { r.ExecutorInstanceId = "" }},
		{"noncanonical identity", func(r *protocol.ReportTaskResultRequest) {
			r.ExecutorInstanceId = strings.ReplaceAll(receiverTestID, "-", "")
		}},
		{"wrong UUID version", func(r *protocol.ReportTaskResultRequest) {
			r.ExecutorInstanceId = "22222222-2222-1222-8222-222222222222"
		}},
		{"wrong UUID variant", func(r *protocol.ReportTaskResultRequest) {
			r.ExecutorInstanceId = "22222222-2222-4222-7222-222222222222"
		}},
		{"missing outcome", func(r *protocol.ReportTaskResultRequest) { r.Outcome = protocol.TaskOutcome_TASK_OUTCOME_UNSPECIFIED }},
		{"unknown outcome", func(r *protocol.ReportTaskResultRequest) { r.Outcome = protocol.TaskOutcome(500) }},
		{"empty JSON", func(r *protocol.ReportTaskResultRequest) { r.Output = &protocol.ReportTaskResultRequest_DataJson{} }},
		{"invalid JSON", func(r *protocol.ReportTaskResultRequest) {
			r.Output = &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte("{")}
		}},
		{"nil output wrapper", func(r *protocol.ReportTaskResultRequest) {
			r.Output = (*protocol.ReportTaskResultRequest_DataJson)(nil)
		}},
		{"missing prepared message", func(r *protocol.ReportTaskResultRequest) {
			r.Output = &protocol.ReportTaskResultRequest_PreparedItems{}
		}},
		{"invalid prepared item", func(r *protocol.ReportTaskResultRequest) {
			r.Output = &protocol.ReportTaskResultRequest_PreparedItems{PreparedItems: &protocol.PreparedItems{Items: [][]byte{[]byte("{")}}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			auth := &receiverAuthStub{groupID: 41}
			acceptor := &receiverAcceptorStub{accepted: true}
			receiver := testReceiver(t, auth, acceptor)
			req := validReceiverRequest()
			test.mutate(req)
			_, err := bizerror.UnaryServerInterceptor(context.Background(), req, &grpc.UnaryServerInfo{},
				func(ctx context.Context, request any) (any, error) {
					return receiver.ReportTaskResult(ctx, request.(*protocol.ReportTaskResultRequest))
				})
			reason, ok := bizerror.SubcodeFromStatus(status.Convert(err))
			if !ok || reason != subcode.InvalidRequest || status.Code(err) != codes.Code(bizerror.DefaultCode) {
				t.Fatalf("wire error = %v, want common.invalid_request", err)
			}
			if len(acceptor.reports) != 0 || auth.calls != 1 {
				t.Fatal("invalid result did not stop between authentication and admission")
			}
		})
	}
}

func TestReceiverJSONPresencePrecisionAndOwnership(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*protocol.ReportTaskResultRequest)
		check     func(*testing.T, taskresult.Report)
	}{
		{"absent output", func(*protocol.ReportTaskResultRequest) {}, func(t *testing.T, r taskresult.Report) {
			if r.Data != nil || r.PreparedItems != nil {
				t.Fatal("absent output was synthesized")
			}
		}},
		{"explicit null", func(r *protocol.ReportTaskResultRequest) {
			r.Output = &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte("null")}
		}, func(t *testing.T, r taskresult.Report) {
			if string(r.Data) != "null" {
				t.Fatal("explicit null was lost")
			}
		}},
		{"number precision", func(r *protocol.ReportTaskResultRequest) {
			r.Output = &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte("9007199254740993")}
		}, func(t *testing.T, r taskresult.Report) {
			if string(r.Data) != "9007199254740993" {
				t.Fatal("large JSON number was converted or aliased")
			}
		}},
		{"JSONB limitation is downstream", func(r *protocol.ReportTaskResultRequest) {
			r.Output = &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte(`"\u0000"`)}
		}, func(t *testing.T, r taskresult.Report) {
			if string(r.Data) != `"\u0000"` {
				t.Fatal("syntactically valid JSON was changed")
			}
		}},
		{"zero prepared items", func(r *protocol.ReportTaskResultRequest) {
			r.Output = &protocol.ReportTaskResultRequest_PreparedItems{PreparedItems: &protocol.PreparedItems{}}
		}, func(t *testing.T, r taskresult.Report) {
			if r.PreparedItems == nil || len(r.PreparedItems) != 0 {
				t.Fatal("explicit empty preparation became absent")
			}
		}},
		{"prepared items", func(r *protocol.ReportTaskResultRequest) {
			r.Output = &protocol.ReportTaskResultRequest_PreparedItems{PreparedItems: &protocol.PreparedItems{Items: [][]byte{[]byte("9007199254740993"), []byte("null")}}}
		}, func(t *testing.T, r taskresult.Report) {
			if len(r.PreparedItems) != 2 || string(r.PreparedItems[0]) != "9007199254740993" || string(r.PreparedItems[1]) != "null" {
				t.Fatal("prepared items were converted or aliased")
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			auth := &receiverAuthStub{groupID: 41}
			acceptor := &receiverAcceptorStub{accepted: true}
			receiver := testReceiver(t, auth, acceptor)
			req := validReceiverRequest()
			test.configure(req)
			if _, err := receiver.ReportTaskResult(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			// A transport owner may reuse its buffers immediately after return.
			switch output := req.Output.(type) {
			case *protocol.ReportTaskResultRequest_DataJson:
				output.DataJson[0] = 'x'
			case *protocol.ReportTaskResultRequest_PreparedItems:
				for _, item := range output.PreparedItems.Items {
					item[0] = 'x'
				}
			}
			test.check(t, acceptor.reports[0])
			if acceptor.reports[0].GroupID != 41 || acceptor.reports[0].ExecutorInstanceID != receiverTestID || acceptor.reports[0].AttemptNo != 4 {
				t.Fatal("trusted or execution identities changed")
			}
		})
	}
}

func TestReceiverIgnoresFailedBusinessOutput(t *testing.T) {
	requests := []*protocol.ReportTaskResultRequest{validReceiverRequest(), validReceiverRequest(), validReceiverRequest()}
	requests[0].Output = &protocol.ReportTaskResultRequest_DataJson{DataJson: []byte("invalid unused JSON")}
	requests[1].Output = &protocol.ReportTaskResultRequest_PreparedItems{PreparedItems: &protocol.PreparedItems{Items: [][]byte{[]byte("invalid unused item")}}}
	requests[2].Output = (*protocol.ReportTaskResultRequest_PreparedItems)(nil)
	for _, req := range requests {
		req.Outcome, req.Summary = protocol.TaskOutcome_FAILURE, "business failed"
		acceptor := &receiverAcceptorStub{accepted: true}
		receiver := testReceiver(t, &receiverAuthStub{groupID: 41}, acceptor)
		if _, err := receiver.ReportTaskResult(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		report := acceptor.reports[0]
		if report.Outcome != taskresult.OutcomeFailure || report.Summary != req.Summary || report.Data != nil || report.PreparedItems != nil {
			t.Fatal("failed result adopted unused output or lost its conclusion")
		}
	}
}

func TestNewReceiverRejectsNilDependencies(t *testing.T) {
	var auth *receiverAuthStub
	var acceptor *receiverAcceptorStub
	for _, test := range []struct {
		auth     GroupAuthenticator
		acceptor ResultAcceptor
	}{
		{nil, &receiverAcceptorStub{}},
		{auth, &receiverAcceptorStub{}},
		{&receiverAuthStub{}, nil},
		{&receiverAuthStub{}, acceptor},
	} {
		if _, err := NewReceiver(test.auth, test.acceptor); err == nil {
			t.Fatal("nil receiver dependency was accepted")
		}
	}
}
