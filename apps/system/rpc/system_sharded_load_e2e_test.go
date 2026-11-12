//go:build integration && e2e && load

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

// TestJobShardedLoadHostProcess keeps the production host and runtime unchanged.
// Only test business is registered; it writes compact audit facts in the isolated
// System table, never Job data. The load-tagged TestMain supplies resource samples.
func TestJobShardedLoadHostProcess(t *testing.T) {
	file := os.Getenv("DOGX_E2E_SYSTEM_CONFIG")
	if file == "" {
		t.Skip("sharded load host is started by the end-to-end test")
	}
	err := run(context.Background(), file, func(dependencies *svc.ServiceContext, registry *jobexecutor.Registry) error {
		processor := &systemShardedLoadProcessor{db: dependencies.DB, worker: os.Getenv("PGAPPNAME")}
		if err := registry.Register("system.e2e.load.ordinary", processor); err != nil {
			return err
		}
		return registry.RegisterSharding("system.e2e.load.sharded", processor)
	})
	if err != nil {
		t.Fatalf("System sharded load host failed: %v", err)
	}
}

type systemShardedLoadProcessor struct {
	db     *gorm.DB
	worker string
}

type systemShardedLoadParams struct {
	Count        int `json:"count"`
	PayloadBytes int `json:"payload_bytes"`
	DelayMS      int `json:"delay_ms"`
}

type systemShardedLoadFact struct {
	Stage, Worker, ParamsHash, ItemHash    string
	Index, PayloadBytes, PreparedWireBytes int
	Total, Succeeded, Failed               int64
	PrepareBuildMilliseconds               float64
}

type systemShardedLoadRecord struct {
	TaskID       int64 `gorm:"column:task_id"`
	InvocationNo int64 `gorm:"column:invocation_no;primaryKey;default:nextval('system_job_load_invocations')"`
	InstanceID   int64 `gorm:"column:instance_id"`
	JobID        int64 `gorm:"column:job_id"`
	Value        string
	State        string
}

func (p *systemShardedLoadProcessor) Execute(ctx context.Context, input jobexecutor.Input) (jobexecutor.Result, error) {
	return p.perform(ctx, input, systemShardedLoadFact{Stage: "ordinary"})
}

func (p *systemShardedLoadProcessor) PreProcess(ctx context.Context, input jobexecutor.Input) (jobexecutor.PrepareResult, error) {
	params, err := parseSystemShardedLoadParams(input.Params)
	if err != nil {
		return jobexecutor.PrepareResult{}, err
	}
	buildStarted := time.Now()
	items := make([]json.RawMessage, params.Count)
	prepared := &protocol.PreparedItems{Items: make([][]byte, params.Count)}
	for index := range items {
		item, err := json.Marshal(struct {
			Index   int    `json:"index"`
			Payload string `json:"payload"`
		}{Index: index, Payload: strings.Repeat("x", params.PayloadBytes)})
		if err != nil {
			return jobexecutor.PrepareResult{}, err
		}
		items[index], prepared.Items[index] = item, item
	}
	const summary = "System load shards prepared."
	// All variable-length identity fields match the real report's encoded size:
	// IDs come from Input, attempt is one, and generated UUIDs/tokens have fixed
	// lengths. No live credential or executor identity is exposed to business.
	wire := &protocol.ReportTaskResultRequest{
		Auth:  &protocol.GroupAuth{GroupName: "system-e2e", Token: strings.Repeat("0", 32)},
		JobId: input.JobID, InstanceId: input.InstanceID, TaskId: input.TaskID,
		AttemptNo: 1, ExecutorInstanceId: "00000000-0000-4000-8000-000000000001",
		Outcome: protocol.TaskOutcome_SUCCESS, Summary: summary,
		Output: &protocol.ReportTaskResultRequest_PreparedItems{PreparedItems: prepared},
	}
	_, err = p.perform(ctx, input, systemShardedLoadFact{Stage: "pre", PreparedWireBytes: proto.Size(wire),
		PrepareBuildMilliseconds: time.Since(buildStarted).Seconds() * 1000})
	return jobexecutor.PrepareResult{Items: items, Summary: summary}, err
}

func (p *systemShardedLoadProcessor) ShardingProcess(ctx context.Context, input jobexecutor.ShardInput) (jobexecutor.Result, error) {
	var item struct {
		Index   int    `json:"index"`
		Payload string `json:"payload"`
	}
	if err := json.Unmarshal(input.Item, &item); err != nil {
		return jobexecutor.Result{}, fmt.Errorf("decode load shard item: %w", err)
	}
	hash, err := systemShardedLoadHash(input.Item)
	if err != nil {
		return jobexecutor.Result{}, err
	}
	return p.perform(ctx, input.Input, systemShardedLoadFact{Stage: "shard", ItemHash: hash, Index: item.Index, PayloadBytes: len(item.Payload)})
}

func (p *systemShardedLoadProcessor) PostProcess(ctx context.Context, input jobexecutor.PostInput) (jobexecutor.Result, error) {
	return p.perform(ctx, input.Input, systemShardedLoadFact{Stage: "post", Total: input.Total, Succeeded: input.Succeeded, Failed: input.Failed})
}

func parseSystemShardedLoadParams(raw string) (systemShardedLoadParams, error) {
	var params systemShardedLoadParams
	if err := json.Unmarshal([]byte(raw), &params); err != nil {
		return params, fmt.Errorf("decode sharded load parameters: %w", err)
	}
	if params.Count < 0 || params.Count > 500 || params.PayloadBytes < 0 || params.PayloadBytes > 8192 || params.DelayMS != 100 {
		return params, errors.New("invalid sharded load parameters")
	}
	return params, nil
}

func (p *systemShardedLoadProcessor) perform(ctx context.Context, input jobexecutor.Input, fact systemShardedLoadFact) (jobexecutor.Result, error) {
	params, err := parseSystemShardedLoadParams(input.Params)
	if err != nil {
		return jobexecutor.Result{}, err
	}
	fact.Worker = p.worker
	paramsHash := sha256.Sum256([]byte(input.Params))
	fact.ParamsHash = hex.EncodeToString(paramsHash[:])
	value, err := json.Marshal(fact)
	if err != nil {
		return jobexecutor.Result{}, err
	}
	// A database sequence gives every call a distinct audit identity across both
	// executor processes. A duplicate business call therefore cannot disappear
	// behind a competing insertion of the same per-process invocation number.
	record := systemShardedLoadRecord{TaskID: input.TaskID, InstanceID: input.InstanceID, JobID: input.JobID, Value: string(value), State: "running"}
	if err := p.db.WithContext(ctx).Table("system_job_e2e_records").Create(&record).Error; err != nil {
		return jobexecutor.Result{}, fmt.Errorf("record sharded load invocation: %w", err)
	}
	timer := time.NewTimer(time.Duration(params.DelayMS) * time.Millisecond)
	defer timer.Stop()
	state := "succeeded"
	select {
	case <-ctx.Done():
		state = "canceled"
	case <-timer.C:
	}
	// Business has already finished or observed cancellation. Test-only audit
	// cleanup must remain observable if cancellation races with this UPDATE;
	// checking ctx.Err first cannot make reuse of the business Context safe.
	// This bounded cleanup does not extend the simulated business delay.
	saveCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := p.db.WithContext(saveCtx).Table("system_job_e2e_records").
		Where("task_id = ? AND invocation_no = ?", input.TaskID, record.InvocationNo).
		Updates(map[string]any{"state": state, "finished_at": gorm.Expr("clock_timestamp()")})
	if result.Error != nil {
		return jobexecutor.Result{}, fmt.Errorf("save sharded load business completion: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return jobexecutor.Result{}, errors.New("sharded load business record is missing")
	}
	return jobexecutor.Result{Summary: "System load business persisted."}, ctx.Err()
}

func systemShardedLoadHash(value []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return "", fmt.Errorf("decode canonical load item: %w", err)
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(canonical)
	return hex.EncodeToString(hash[:]), nil
}
