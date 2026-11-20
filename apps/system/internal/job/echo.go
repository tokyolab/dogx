// Package job contains System-owned business processors for scheduled tasks.
package job

import (
	"context"

	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/zeromicro/go-zero/core/logx"
)

// EchoProcessor logs a fixed greeting for the demo.echo ordinary task.
// It ignores task parameters and keeps no per-execution state.
type EchoProcessor struct{}

// Execute logs the greeting and returns it as the execution summary.
func (EchoProcessor) Execute(ctx context.Context, input jobexecutor.Input) (jobexecutor.Result, error) {
	if err := ctx.Err(); err != nil {
		return jobexecutor.Result{}, err
	}
	logx.WithContext(ctx).Infow("hello world",
		logx.Field("job_id", input.JobID),
		logx.Field("instance_id", input.InstanceID),
		logx.Field("task_id", input.TaskID),
	)
	return jobexecutor.Result{Summary: "hello world"}, nil
}
