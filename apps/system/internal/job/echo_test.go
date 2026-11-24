package job

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/zeromicro/go-zero/core/logx"
)

func TestEchoProcessorExecute(t *testing.T) {
	for _, params := range []string{"", "synthetic-private-parameters"} {
		t.Run(params, func(t *testing.T) {
			logs := captureEchoLogs(t)
			input := jobexecutor.Input{JobID: 11, InstanceID: 22, TaskID: 33, Params: params}
			result, err := (EchoProcessor{}).Execute(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			if result.Summary != "hello world" || result.Data != nil {
				t.Fatalf("unexpected echo result: %+v", result)
			}
			var entry struct {
				Content    string `json:"content"`
				Level      string `json:"level"`
				JobID      int64  `json:"job_id"`
				InstanceID int64  `json:"instance_id"`
				TaskID     int64  `json:"task_id"`
			}
			if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
				t.Fatalf("decode greeting log: %v", err)
			}
			if entry.Content != "hello world" || entry.Level != "info" ||
				entry.JobID != input.JobID || entry.InstanceID != input.InstanceID || entry.TaskID != input.TaskID {
				t.Fatalf("greeting log lost its content or task identity: %+v", entry)
			}
			if params != "" && strings.Contains(logs.String(), params) {
				t.Fatal("greeting log exposed ignored task parameters")
			}
		})
	}
}

func TestEchoProcessorHonorsContextError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "expired", err: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs := captureEchoLogs(t)
			var ctx context.Context
			var cancel context.CancelFunc
			if errors.Is(test.err, context.Canceled) {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			} else {
				ctx, cancel = context.WithDeadline(context.Background(), time.Time{})
			}
			defer cancel()
			result, err := (EchoProcessor{}).Execute(ctx, jobexecutor.Input{JobID: 11, InstanceID: 22, TaskID: 33})
			if !errors.Is(err, test.err) || result.Summary != "" || result.Data != nil {
				t.Fatalf("echo ignored context error: result=%+v error=%v", result, err)
			}
			if logs.Len() != 0 {
				t.Fatal("echo logged a greeting after context cancellation or expiry")
			}
		})
	}
}

// captureEchoLogs replaces the process-wide writer, so its callers must remain
// sequential. Cleanup restores the previous writer for other processor tests.
func captureEchoLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := logx.Reset()
	logx.SetWriter(logx.NewWriter(&logs))
	t.Cleanup(func() { logx.SetWriter(previous) })
	return &logs
}
