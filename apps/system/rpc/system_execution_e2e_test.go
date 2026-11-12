//go:build integration && e2e

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"gorm.io/gorm"
)

// TestJobExecutorHostProcess runs only as the isolated System test process.
// It uses production server assembly and dependency initialization, injecting
// test business behavior through the same processor registration callback.
func TestJobExecutorHostProcess(t *testing.T) {
	file := os.Getenv("DOGX_E2E_SYSTEM_CONFIG")
	if file == "" {
		t.Skip("System execution host is started by the end-to-end test")
	}
	err := run(context.Background(), file, func(dependencies *svc.ServiceContext, registry *jobexecutor.Registry) error {
		if err := registry.Register("system.e2e.persist", &systemExecutionProcessor{
			database: dependencies.DB, invocations: make(map[int64]int64),
		}); err != nil {
			return err
		}
		return registry.RegisterSharding("system.e2e.sharded", &systemShardedExecutionProcessor{
			records: &systemExecutionProcessor{database: dependencies.DB, invocations: make(map[int64]int64)},
		})
	})
	if err != nil {
		t.Fatalf("System execution host failed: %v", err)
	}
}

// systemExecutionProcessor records actual business calls in the isolated System
// schema. Its local invocation number is not the scheduler's attempt identity,
// which the public Processor contract deliberately does not expose. All state
// belongs to this test process; no production processor or test switch is added.
type systemExecutionProcessor struct {
	database    *gorm.DB
	mu          sync.Mutex
	invocations map[int64]int64
}

func (p *systemExecutionProcessor) Execute(ctx context.Context, input jobexecutor.Input) (jobexecutor.Result, error) {
	var params struct {
		Mode    string `json:"mode"`
		Value   string `json:"value"`
		DelayMS int    `json:"delay_ms"`
	}
	if err := json.Unmarshal([]byte(input.Params), &params); err != nil {
		return jobexecutor.Result{}, errors.New("invalid E2E business parameters")
	}
	if params.Mode != "success" && params.Mode != "retry" && params.Mode != "wait" && params.Mode != "gated" && params.Mode != "stubborn" && params.Mode != "load" {
		return jobexecutor.Result{}, errors.New("unsupported E2E business mode")
	}
	if params.Mode == "load" && (params.DelayMS < 1 || params.DelayMS > 1000) {
		return jobexecutor.Result{}, errors.New("load business delay must be between 1 and 1000 milliseconds")
	}
	p.mu.Lock()
	p.invocations[input.TaskID]++
	invocation := p.invocations[input.TaskID]
	p.mu.Unlock()
	row := map[string]any{
		"task_id": input.TaskID, "invocation_no": invocation,
		"instance_id": input.InstanceID, "job_id": input.JobID,
		"value": params.Value, "state": "running",
	}
	if err := p.database.WithContext(ctx).Table("system_job_e2e_records").Create(row).Error; err != nil {
		return jobexecutor.Result{}, fmt.Errorf("record E2E business invocation: %w", err)
	}
	if params.Mode == "load" {
		// A fixed wait models I/O latency without holding a database connection.
		// The load fixture's database timestamps bracket this actual invocation.
		timer := time.NewTimer(time.Duration(params.DelayMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return jobexecutor.Result{}, ctx.Err()
		case <-timer.C:
		}
		result := p.database.WithContext(ctx).Table("system_job_e2e_records").
			Where("task_id = ? AND invocation_no = ?", input.TaskID, invocation).
			Updates(map[string]any{"state": "succeeded", "finished_at": gorm.Expr("clock_timestamp()")})
		if result.Error != nil {
			return jobexecutor.Result{}, fmt.Errorf("save load business completion: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return jobexecutor.Result{}, errors.New("load business invocation record is missing")
		}
		return jobexecutor.Result{Summary: "Load business persisted.", Data: json.RawMessage(`{"completed":true}`)}, nil
	}
	if params.Mode == "gated" {
		// Only the test's System business record controls this gate. Shutdown
		// must keep the database and report client alive until the accepted call
		// actually finishes; the test releases it after observing stopped admission.
		if err := p.waitForRelease(ctx, input.TaskID, invocation); err != nil {
			return jobexecutor.Result{}, err
		}
	}
	if params.Mode == "stubborn" {
		if err := p.waitIgnoringCancellation(input.TaskID, invocation); err != nil {
			return jobexecutor.Result{}, err
		}
	}
	if params.Mode == "wait" {
		<-ctx.Done()
		// The business Context is already canceled. A separate bounded cleanup
		// records actual observation of cancellation before this call returns.
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return jobexecutor.Result{}, errors.Join(ctx.Err(), p.setState(cleanup, input.TaskID, invocation, "canceled"))
	}
	if params.Mode == "retry" && invocation == 1 {
		if err := p.setState(ctx, input.TaskID, invocation, "failed"); err != nil {
			return jobexecutor.Result{}, err
		}
		return jobexecutor.Result{}, errors.New("E2E business failed its first invocation")
	}
	if err := p.setState(ctx, input.TaskID, invocation, "succeeded"); err != nil {
		return jobexecutor.Result{}, err
	}
	data, err := json.Marshal(struct {
		Mode         string `json:"mode"`
		Value        string `json:"value"`
		InvocationNo int64  `json:"invocation_no"`
	}{Mode: params.Mode, Value: params.Value, InvocationNo: invocation})
	if err != nil {
		return jobexecutor.Result{}, fmt.Errorf("encode E2E business result: %w", err)
	}
	return jobexecutor.Result{Summary: "System E2E business persisted.", Data: data}, nil
}

func (p *systemExecutionProcessor) waitForRelease(ctx context.Context, taskID, invocation int64) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var record struct{ State string }
		err := p.database.WithContext(ctx).Table("system_job_e2e_records").
			Select("state").Where("task_id = ? AND invocation_no = ?", taskID, invocation).Take(&record).Error
		if err != nil {
			return fmt.Errorf("read E2E business release: %w", err)
		}
		if record.State == "release" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// waitIgnoringCancellation models business code that fails to honor its caller's
// cancellation. Its independent, bounded polling cannot keep the test suite alive
// indefinitely. A single parent-requested probe acknowledges database access
// during shutdown without adding continuous writes or production test switches.
func (p *systemExecutionProcessor) waitIgnoringCancellation(taskID, invocation int64) error {
	deadline, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(deadline, time.Second)
		var record struct{ State string }
		err := p.database.WithContext(ctx).Table("system_job_e2e_records").
			Select("state").Where("task_id = ? AND invocation_no = ?", taskID, invocation).Take(&record).Error
		if err == nil && record.State == "probe" {
			err = p.setState(ctx, taskID, invocation, "running")
		}
		cancel()
		if err != nil {
			return fmt.Errorf("access E2E business during shutdown: %w", err)
		}
		if record.State == "release" {
			return nil
		}
		select {
		case <-deadline.Done():
			return deadline.Err()
		case <-ticker.C:
		}
	}
}

func (p *systemExecutionProcessor) setState(ctx context.Context, taskID, invocation int64, state string) error {
	result := p.database.WithContext(ctx).Table("system_job_e2e_records").
		Where("task_id = ? AND invocation_no = ?", taskID, invocation).Update("state", state)
	if result.Error != nil {
		return fmt.Errorf("save E2E business state: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return errors.New("E2E business invocation record is missing")
	}
	return nil
}
