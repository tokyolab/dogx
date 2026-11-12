// Package jobexecutor defines business processing contracts and the reusable
// executor component. Business processors do not access scheduler tables or
// implement dispatch, result reporting, cancellation routing, or retry policy.
package jobexecutor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Processor executes ordinary Tasks. The same instance may be called
// concurrently, so per-execution state belongs in local variables. Returning
// means all business work for this call has ended, including work started in
// other goroutines; success must not be returned while that work continues.
//
// The executor supplies a Context independent of the dispatch RPC and bounded
// by the original Instance deadline. Cancellation is cooperative and does not
// prove that business work has exited. Retries may execute in another process,
// and the processor is responsible for business idempotency.
// Returned output transfers to the executor; processors must not mutate its
// slices after returning, including from goroutines they started.
type Processor interface {
	// Execute returns nil error on business success, including a zero Result.
	// A non-nil error reports failure and causes returned Data to be ignored.
	// It does not control the scheduler's retry budget. Success does not prove
	// that the result has been reported, persisted, or accepted by the Instance.
	Execute(ctx context.Context, input Input) (Result, error)
}

// ShardingProcessor defines the final shards and executes one item per Task.
// Both methods belong to one named processor. Preparation, shards, retries,
// and optional Post work may run concurrently or in different processes;
// processor fields cannot carry an Instance's state between these stages.
// The Context, completion, output ownership and idempotency requirements of
// Processor apply.
type ShardingProcessor interface {
	// PreProcess must explicitly return a non-nil Items collection on success.
	// Each element defines one final shard Task; the scheduler neither splits
	// nor combines elements. An empty collection means zero shards; nil does
	// not. On error, partial items are ignored and authorize no shard Tasks.
	PreProcess(ctx context.Context, input Input) (PrepareResult, error)
	// ShardingProcess executes the single item assigned to this Task. A nil error
	// indicates success; returned Data is ignored when an error is present.
	ShardingProcess(ctx context.Context, input ShardInput) (Result, error)
}

// PostProcessor adds optional business finalization to a ShardingProcessor.
// Its presence does not enable Post: the frozen Instance configuration must
// request it. Ordinary processors never run Post. It is not a guaranteed exit
// hook for failed preparation, manual stop, or Instance timeout.
type PostProcessor interface {
	// PostProcess receives only final shard counts and the common input. The
	// business queries its own persisted records for details when needed.
	// It uses the original Instance deadline, and its result participates in
	// the Instance outcome; success cannot erase failed shard outcomes.
	PostProcess(ctx context.Context, input PostInput) (Result, error)
}

// Input identifies the current business Task and its frozen Instance input.
// Attempt identity and machine credentials are owned by the executor and are
// deliberately not exposed as processor-managed reporting fields.
type Input struct {
	JobID      int64
	InstanceID int64
	TaskID     int64
	Params     string
}

// ShardInput separates frozen Job parameters from this Task's single JSON item.
// Item is the value of one element of PrepareResult.Items, without an outer
// array wrapper. It may contain any JSON value, including an array or JSON null;
// missing or empty bytes are invalid. Its structure is defined by the processor.
type ShardInput struct {
	Input
	Item json.RawMessage
}

// PostInput contains final counts of business shard Tasks only, excluding
// preparation and Post Tasks. All counts are nonnegative and Total equals
// Succeeded plus Failed. It exposes neither per-shard results nor final Instance
// status. A valid zero-shard execution supplies three zero counts.
type PostInput struct {
	Input
	Total     int64
	Succeeded int64
	Failed    int64
}

// Result is the business output of ordinary, shard, or Post processing.
// A nil Data omits the value; JSON null is explicitly encoded as []byte("null").
// Other provided values must be valid JSON, without converting numbers through
// float64. Summary is a readable explanation, not a detailed execution log.
type Result struct {
	Data    json.RawMessage
	Summary string
}

// Validate checks the JSON syntax of a successful Result. It does not prove
// that PostgreSQL can store it as JSONB or that it fits a transport limit.
// Callers must ignore Data instead of calling Validate when processing failed.
func (r Result) Validate() error {
	if r.Data != nil && !json.Valid(r.Data) {
		return errors.New("result data must contain valid JSON")
	}
	return nil
}

// MaxPreparedItems limits the number of business shard Tasks in one Instance.
// Preparation and Post Tasks are not included in this limit.
const MaxPreparedItems = 500

// PrepareResult explicitly separates final shard inputs from ordinary Data.
// Each element creates one Task, even if elements have identical JSON values.
// A non-nil empty Items collection represents successful zero-shard preparation.
// Nil Items, invalid JSON, or more than MaxPreparedItems elements are not valid
// successful output. On business failure, all returned items are ignored instead
// of being partially adopted.
type PrepareResult struct {
	Items   []json.RawMessage
	Summary string
}

// Validate checks successful preparation output without modifying its JSON.
// It enforces MaxPreparedItems and preserves the distinction between missing
// Items and an explicit empty collection. It does not validate business values,
// transport size, or JSONB storage constraints. Failed preparation must ignore
// Items without this check.
func (r PrepareResult) Validate() error {
	if r.Items == nil {
		return errors.New("prepared items must be explicitly provided")
	}
	if len(r.Items) > MaxPreparedItems {
		return fmt.Errorf("prepared items must not exceed %d elements", MaxPreparedItems)
	}
	for index, item := range r.Items {
		if !json.Valid(item) {
			return fmt.Errorf("prepared item at index %d must contain valid JSON", index)
		}
	}
	return nil
}
