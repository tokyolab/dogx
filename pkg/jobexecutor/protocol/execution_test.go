package protocol

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDispatchWirePreservesStageAndIdentity(t *testing.T) {
	request := &DispatchTaskRequest{
		JobId: 9007199254740993, InstanceId: 9007199254740994,
		TaskId: 9007199254740995, AttemptNo: 2,
		ProcessorName: "system.cleanup", Params: " frozen params ",
		DeadlineAt: timestamppb.New(time.Date(2026, 11, 4, 8, 0, 0, 123, time.UTC)),
	}
	cases := []struct {
		name  string
		input isDispatchTaskRequest_Input
	}{
		{name: "absent"},
		{name: "ordinary", input: &DispatchTaskRequest_Ordinary{Ordinary: &OrdinaryInput{}}},
		{name: "prepare", input: &DispatchTaskRequest_Prepare{Prepare: &PrepareInput{}}},
		{name: "shard with missing value", input: &DispatchTaskRequest_Shard{Shard: &ShardInput{}}},
		{name: "shard with JSON null", input: &DispatchTaskRequest_Shard{Shard: &ShardInput{Item: []byte(`null`)}}},
		{name: "shard with large integer", input: &DispatchTaskRequest_Shard{Shard: &ShardInput{Item: []byte(`9007199254740993`)}}},
		{name: "shard with object", input: &DispatchTaskRequest_Shard{Shard: &ShardInput{Item: []byte(`{"id":9007199254740993}`)}}},
		{name: "shard with nested array", input: &DispatchTaskRequest_Shard{Shard: &ShardInput{Item: []byte(`[[1,2],{"ids":[3,4]},null]`)}}},
		{name: "post", input: &DispatchTaskRequest_Post{Post: &PostInput{Total: 3, Succeeded: 2, Failed: 1}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request.Input = test.input
			wire, err := proto.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			var decoded DispatchTaskRequest
			if err := proto.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(request, &decoded) {
				t.Fatal("wire round trip changed dispatch presence, identity, deadline, or input")
			}
			if request.Input == nil && decoded.GetInput() != nil {
				t.Fatal("absent input became an ordinary task")
			}
		})
	}
}

func TestResultWirePreservesOutputPresenceAndRawJSON(t *testing.T) {
	cases := []struct {
		name   string
		output isReportTaskResultRequest_Output
	}{
		{name: "absent"},
		{name: "explicit null", output: &ReportTaskResultRequest_DataJson{DataJson: []byte(`null`)}},
		{name: "large integer", output: &ReportTaskResultRequest_DataJson{DataJson: []byte(`9007199254740993`)}},
		{name: "explicit empty data", output: &ReportTaskResultRequest_DataJson{DataJson: []byte{}}},
		{name: "explicit zero shards", output: &ReportTaskResultRequest_PreparedItems{PreparedItems: &PreparedItems{}}},
		{name: "prepared values", output: &ReportTaskResultRequest_PreparedItems{PreparedItems: &PreparedItems{Items: [][]byte{[]byte(`9007199254740993`), []byte(`null`)}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := &ReportTaskResultRequest{Outcome: TaskOutcome_SUCCESS, Output: test.output}
			wire, err := proto.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			var decoded ReportTaskResultRequest
			if err := proto.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(request, &decoded) {
				t.Fatal("wire round trip changed result presence or payload")
			}
			switch output := test.output.(type) {
			case nil:
				if decoded.GetOutput() != nil {
					t.Fatal("absent output became a present result")
				}
			case *ReportTaskResultRequest_DataJson:
				if _, present := decoded.GetOutput().(*ReportTaskResultRequest_DataJson); !present || !bytes.Equal(decoded.GetDataJson(), output.DataJson) {
					t.Fatal("raw Data presence or encoding changed")
				}
			case *ReportTaskResultRequest_PreparedItems:
				if decoded.GetPreparedItems() == nil {
					t.Fatal("explicit zero-shard preparation became missing output")
				}
			}
		})
	}
}
