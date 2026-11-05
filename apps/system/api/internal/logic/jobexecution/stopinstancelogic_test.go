package jobexecution

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStopInstanceAPIForwardsObservedStateWithoutReinterpretingIt(t *testing.T) {
	manual, timeout, empty := "manual_stop", "instance_timeout", ""
	for _, test := range []struct {
		state  int32
		reason *string
	}{{6, &manual}, {6, &timeout}, {3, nil}, {4, &empty}, {5, &manual}} {
		client := &executionRPCStub{stop: &jobclient.StopInstanceResponse{InstanceId: 9, Status: test.state, EndReason: test.reason}}
		logic := NewStopInstanceLogic(context.Background(), &svc.ServiceContext{JobRpc: client})
		result, err := logic.StopInstance(&types.IDReq{Id: 9})
		if err != nil || client.stopRequest.Id != 9 || result.InstanceId != 9 || result.Status != test.state || result.EndReason != test.reason {
			t.Fatalf("stop response facts changed: %+v %v", result, err)
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		if test.reason == nil && string(fields["endReason"]) != "null" {
			t.Fatal("absent end reason was invented or omitted")
		}
	}
}

func TestStopInstanceAPIPreservesErrorsAndRejectsNilSuccess(t *testing.T) {
	client := &executionRPCStub{}
	logic := NewStopInstanceLogic(context.Background(), &svc.ServiceContext{JobRpc: client})
	if response, err := logic.StopInstance(nil); response != nil || status.Code(err) != codes.InvalidArgument || client.stopRequest != nil {
		t.Fatal("nil stop request reached RPC")
	}
	if response, err := logic.StopInstance(&types.IDReq{Id: 9}); response != nil || status.Code(err) != codes.Internal {
		t.Fatal("nil stop response became success")
	}
	cause := errors.New("synthetic stop RPC failure")
	client.err = cause
	if response, err := logic.StopInstance(&types.IDReq{Id: 9}); response != nil || !errors.Is(err, cause) {
		t.Fatal("stop RPC error was replaced")
	}
}
