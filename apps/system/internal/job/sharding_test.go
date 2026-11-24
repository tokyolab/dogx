package job

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/pkg/jobexecutor"
)

func TestShardingProcessorLifecycle(t *testing.T) {
	processor := ShardingProcessor{}
	input := jobexecutor.Input{JobID: 11, InstanceID: 22, TaskID: 33, Params: "ignored demo parameters"}
	prepared, err := processor.PreProcess(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Validate(); err != nil {
		t.Fatalf("invalid prepared items: %v", err)
	}
	names := []string{"Alice", "Bob", "Carol"}
	if len(prepared.Items) != len(names) || prepared.Summary != "prepared 3 shards" {
		t.Fatalf("unexpected preparation: %+v", prepared)
	}
	for index, name := range names {
		shardInput := input
		shardInput.TaskID = int64(34 + index)
		// A fresh processor demonstrates that stages need no shared state.
		result, err := (ShardingProcessor{}).ShardingProcess(context.Background(), jobexecutor.ShardInput{
			Input: shardInput, Item: prepared.Items[index],
		})
		if err != nil {
			t.Fatalf("execute shard %d: %v", index, err)
		}
		var data map[string]string
		if err := json.Unmarshal(result.Data, &data); err != nil {
			t.Fatalf("decode shard %d result: %v", index, err)
		}
		if data["name"] != name || data["message"] != "hello "+name || result.Summary != "hello "+name {
			t.Fatalf("unexpected shard %d result: %+v", index, result)
		}
	}
	post, err := (ShardingProcessor{}).PostProcess(context.Background(), jobexecutor.PostInput{
		Input: input, Total: 3, Succeeded: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	var counts map[string]int64
	if err := json.Unmarshal(post.Data, &counts); err != nil {
		t.Fatal(err)
	}
	if len(counts) != 3 || counts["total"] != 3 || counts["succeeded"] != 3 || counts["failed"] != 0 ||
		post.Summary != "completed 3 shards: 3 succeeded, 0 failed" {
		t.Fatalf("unexpected Post result: %+v", post)
	}
}

func TestShardingProcessorRejectsInvalidItems(t *testing.T) {
	for _, item := range []string{
		``, `{"name":`, `null`, `[]`, `{}`, `{"name":""}`, `{"name":" \t "}`, `{"name":123}`,
	} {
		t.Run(item, func(t *testing.T) {
			result, err := (ShardingProcessor{}).ShardingProcess(context.Background(), jobexecutor.ShardInput{
				Item: json.RawMessage(item),
			})
			if err == nil || result.Data != nil || result.Summary != "" {
				t.Fatalf("invalid item produced success output: result=%+v error=%v", result, err)
			}
		})
	}
	_, err := (ShardingProcessor{}).ShardingProcess(context.Background(), jobexecutor.ShardInput{
		Item: json.RawMessage(`{"name":`),
	})
	var syntaxError *json.SyntaxError
	if !errors.As(err, &syntaxError) {
		t.Fatalf("JSON error cause was lost: %v", err)
	}
}

func TestShardingProcessorPostUsesSuppliedCounts(t *testing.T) {
	for _, test := range []struct {
		name    string
		input   jobexecutor.PostInput
		summary string
	}{
		{name: "mixed outcomes", input: jobexecutor.PostInput{Total: 7, Succeeded: 5, Failed: 2}, summary: "completed 7 shards: 5 succeeded, 2 failed"},
		{name: "no shards", input: jobexecutor.PostInput{}, summary: "completed 0 shards: 0 succeeded, 0 failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := (ShardingProcessor{}).PostProcess(context.Background(), test.input)
			if err != nil {
				t.Fatal(err)
			}
			var counts map[string]int64
			if err := json.Unmarshal(result.Data, &counts); err != nil {
				t.Fatal(err)
			}
			if len(counts) != 3 || counts["total"] != test.input.Total || counts["succeeded"] != test.input.Succeeded ||
				counts["failed"] != test.input.Failed || result.Summary != test.summary {
				t.Fatalf("Post changed the supplied counts: %+v", result)
			}
		})
	}
}

func TestShardingProcessorHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	processor := ShardingProcessor{}
	prepared, err := processor.PreProcess(ctx, jobexecutor.Input{})
	if !errors.Is(err, context.Canceled) || prepared.Items != nil || prepared.Summary != "" {
		t.Fatalf("preparation ignored cancellation: result=%+v error=%v", prepared, err)
	}
	result, err := processor.ShardingProcess(ctx, jobexecutor.ShardInput{Item: json.RawMessage(`{"name":"Alice"}`)})
	if !errors.Is(err, context.Canceled) || result.Data != nil || result.Summary != "" {
		t.Fatalf("shard ignored cancellation: result=%+v error=%v", result, err)
	}
	result, err = processor.PostProcess(ctx, jobexecutor.PostInput{Total: 3, Succeeded: 3})
	if !errors.Is(err, context.Canceled) || result.Data != nil || result.Summary != "" {
		t.Fatalf("Post ignored cancellation: result=%+v error=%v", result, err)
	}
}
