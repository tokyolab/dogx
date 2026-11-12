//go:build integration && e2e

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"gorm.io/gorm"
)

// systemShardedExecutionProcessor persists the actual inputs and outcomes of
// test business calls in the isolated System table. Stages share no in-memory
// business data; the local invocation counter only detects repeated calls to
// the same Task. The processor never reads or writes Job tables.
type systemShardedExecutionProcessor struct {
	records *systemExecutionProcessor
}

type systemShardedExecutionParams struct {
	Mode  string            `json:"mode"`
	Value string            `json:"value"`
	Items []json.RawMessage `json:"items"`
}

type systemShardedExecutionInput struct {
	Stage  string                        `json:"stage"`
	Params string                        `json:"params"`
	Item   json.RawMessage               `json:"item,omitempty"`
	Post   *systemShardedExecutionCounts `json:"post,omitempty"`
}

type systemShardedExecutionCounts struct {
	Total     int64 `json:"total"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
}

func (p *systemShardedExecutionProcessor) PreProcess(ctx context.Context, input jobexecutor.Input) (jobexecutor.PrepareResult, error) {
	params, err := systemShardedParams(input)
	if err != nil {
		return jobexecutor.PrepareResult{}, err
	}
	invocation, err := p.begin(ctx, input, systemShardedExecutionInput{Stage: "pre", Params: input.Params})
	if err != nil {
		return jobexecutor.PrepareResult{}, err
	}
	if err := p.finish(ctx, input.TaskID, invocation, "succeeded"); err != nil {
		return jobexecutor.PrepareResult{}, err
	}
	return jobexecutor.PrepareResult{Items: params.Items, Summary: "System E2E shards prepared."}, nil
}

func (p *systemShardedExecutionProcessor) ShardingProcess(ctx context.Context, input jobexecutor.ShardInput) (jobexecutor.Result, error) {
	params, err := systemShardedParams(input.Input)
	if err != nil {
		return jobexecutor.Result{}, err
	}
	invocation, err := p.begin(ctx, input.Input, systemShardedExecutionInput{Stage: "shard", Params: input.Params, Item: input.Item})
	if err != nil {
		return jobexecutor.Result{}, err
	}
	if params.Mode == "gated" {
		// The parent releases only System business records after observing the
		// scheduler's OS exit. Neither process manufactures an executor report.
		if err := p.records.waitForRelease(ctx, input.TaskID, invocation); err != nil {
			return jobexecutor.Result{}, err
		}
	}
	if params.Mode == "wait" {
		<-ctx.Done()
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return jobexecutor.Result{}, errors.Join(ctx.Err(), p.finish(cleanup, input.TaskID, invocation, "canceled"))
	}
	var itemName string
	if params.Mode == "shard_retry" && invocation == 1 && json.Unmarshal(input.Item, &itemName) == nil && itemName == "first" {
		if err := p.finish(ctx, input.TaskID, invocation, "failed"); err != nil {
			return jobexecutor.Result{}, err
		}
		return jobexecutor.Result{}, errors.New("System E2E shard failed its first invocation")
	}
	if err := p.finish(ctx, input.TaskID, invocation, "succeeded"); err != nil {
		return jobexecutor.Result{}, err
	}
	return jobexecutor.Result{Summary: "System E2E shard persisted.", Data: json.RawMessage(`{"completed":true}`)}, nil
}

func (p *systemShardedExecutionProcessor) PostProcess(ctx context.Context, input jobexecutor.PostInput) (jobexecutor.Result, error) {
	params, err := systemShardedParams(input.Input)
	if err != nil {
		return jobexecutor.Result{}, err
	}
	invocation, err := p.begin(ctx, input.Input, systemShardedExecutionInput{
		Stage: "post", Params: input.Params,
		Post: &systemShardedExecutionCounts{Total: input.Total, Succeeded: input.Succeeded, Failed: input.Failed},
	})
	if err != nil {
		return jobexecutor.Result{}, err
	}
	if params.Mode == "post_retry" && invocation == 1 {
		if err := p.finish(ctx, input.TaskID, invocation, "failed"); err != nil {
			return jobexecutor.Result{}, err
		}
		return jobexecutor.Result{}, errors.New("System E2E Post failed its first invocation")
	}
	if err := p.finish(ctx, input.TaskID, invocation, "succeeded"); err != nil {
		return jobexecutor.Result{}, err
	}
	return jobexecutor.Result{Summary: "System E2E Post persisted.", Data: json.RawMessage(`{"completed":true}`)}, nil
}

func systemShardedParams(input jobexecutor.Input) (systemShardedExecutionParams, error) {
	var params systemShardedExecutionParams
	if err := json.Unmarshal([]byte(input.Params), &params); err != nil {
		return params, fmt.Errorf("decode System E2E sharded parameters: %w", err)
	}
	switch params.Mode {
	case "success", "shard_retry", "post_retry", "gated", "wait":
	default:
		return params, errors.New("unsupported System E2E sharded mode")
	}
	if params.Items == nil {
		return params, errors.New("System E2E shard items must be explicitly provided")
	}
	return params, nil
}

func (p *systemShardedExecutionProcessor) begin(ctx context.Context, input jobexecutor.Input, captured systemShardedExecutionInput) (int64, error) {
	value, err := json.Marshal(captured)
	if err != nil {
		return 0, fmt.Errorf("encode System E2E stage input: %w", err)
	}
	p.records.mu.Lock()
	p.records.invocations[input.TaskID]++
	invocation := p.records.invocations[input.TaskID]
	p.records.mu.Unlock()
	row := map[string]any{
		"task_id": input.TaskID, "invocation_no": invocation,
		"instance_id": input.InstanceID, "job_id": input.JobID,
		"value": string(value), "state": "running",
	}
	if err := p.records.database.WithContext(ctx).Table("system_job_e2e_records").Create(row).Error; err != nil {
		return 0, fmt.Errorf("record System E2E stage invocation: %w", err)
	}
	return invocation, nil
}

func (p *systemShardedExecutionProcessor) finish(ctx context.Context, taskID, invocation int64, state string) error {
	result := p.records.database.WithContext(ctx).Table("system_job_e2e_records").
		Where("task_id = ? AND invocation_no = ?", taskID, invocation).
		Updates(map[string]any{"state": state, "finished_at": gorm.Expr("clock_timestamp()")})
	if result.Error != nil {
		return fmt.Errorf("save System E2E stage completion: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return errors.New("System E2E stage invocation record is missing")
	}
	return nil
}
