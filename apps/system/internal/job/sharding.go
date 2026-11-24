package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/zeromicro/go-zero/core/logx"
)

// ShardingProcessor demonstrates preparation, shard execution and optional Post
// processing for demo.sharding. It ignores task parameters and keeps no state
// between stages, which may execute concurrently or in different processes.
type ShardingProcessor struct{}

// PreProcess returns three independent greeting inputs, one per shard Task.
func (ShardingProcessor) PreProcess(ctx context.Context, _ jobexecutor.Input) (jobexecutor.PrepareResult, error) {
	if err := ctx.Err(); err != nil {
		return jobexecutor.PrepareResult{}, err
	}
	return jobexecutor.PrepareResult{
		Items: []json.RawMessage{
			json.RawMessage(`{"name":"Alice"}`),
			json.RawMessage(`{"name":"Bob"}`),
			json.RawMessage(`{"name":"Carol"}`),
		},
		Summary: "prepared 3 shards",
	}, nil
}

// ShardingProcess requires a JSON object with a nonblank name and returns its
// greeting. Malformed JSON and missing names fail only this business attempt.
func (ShardingProcessor) ShardingProcess(ctx context.Context, input jobexecutor.ShardInput) (jobexecutor.Result, error) {
	if err := ctx.Err(); err != nil {
		return jobexecutor.Result{}, err
	}
	var item struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(input.Item, &item); err != nil {
		return jobexecutor.Result{}, fmt.Errorf("decode demo shard: %w", err)
	}
	item.Name = strings.TrimSpace(item.Name)
	if item.Name == "" {
		return jobexecutor.Result{}, errors.New("demo shard name must not be empty")
	}
	greeting := "hello " + item.Name
	data, err := json.Marshal(map[string]string{"name": item.Name, "message": greeting})
	if err != nil {
		return jobexecutor.Result{}, fmt.Errorf("encode demo shard result: %w", err)
	}
	logx.WithContext(ctx).Infow(greeting,
		logx.Field("job_id", input.JobID),
		logx.Field("instance_id", input.InstanceID),
		logx.Field("task_id", input.TaskID),
	)
	return jobexecutor.Result{Data: data, Summary: greeting}, nil
}

// PostProcess records the supplied final shard counts without reading individual
// results. A successful Post does not change the outcome of failed shards.
func (ShardingProcessor) PostProcess(ctx context.Context, input jobexecutor.PostInput) (jobexecutor.Result, error) {
	if err := ctx.Err(); err != nil {
		return jobexecutor.Result{}, err
	}
	data, err := json.Marshal(map[string]int64{
		"total": input.Total, "succeeded": input.Succeeded, "failed": input.Failed,
	})
	if err != nil {
		return jobexecutor.Result{}, fmt.Errorf("encode demo shard counts: %w", err)
	}
	summary := fmt.Sprintf("completed %d shards: %d succeeded, %d failed", input.Total, input.Succeeded, input.Failed)
	logx.WithContext(ctx).Infow("demo sharding completed",
		logx.Field("job_id", input.JobID),
		logx.Field("instance_id", input.InstanceID),
		logx.Field("task_id", input.TaskID),
		logx.Field("total", input.Total),
		logx.Field("succeeded", input.Succeeded),
		logx.Field("failed", input.Failed),
	)
	return jobexecutor.Result{Data: data, Summary: summary}, nil
}
