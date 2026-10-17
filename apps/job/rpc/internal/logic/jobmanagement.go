package logic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func jobManagementError(operation string, err error) error {
	switch {
	case errors.Is(err, repository.ErrJobNotFound):
		return bizerror.New(subcode.JobNotFound, "任务不存在")
	case errors.Is(err, repository.ErrJobStateNotAllowed):
		return bizerror.New(subcode.JobStateNotAllowed, "请先停用任务再进行修改")
	case errors.Is(err, repository.ErrExecutorGroupUnavailable):
		return bizerror.New(subcode.JobExecutorGroupUnavailable, "执行器组不可用")
	default:
		return rpcOperationError(operation, err)
	}
}

func rpcOperationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	// Preserve the cause for in-process diagnostics while retaining the RPC
	// classification; a wrapped Context error otherwise becomes Unknown.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: %w: %w", operation, status.FromContextError(err).Err(), err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func missingJobRecord() error {
	return status.Error(codes.Internal, "job repository returned a nil record without an error")
}

func notifyJobChanged(ctx context.Context, notify func(context.Context) error) {
	if notify == nil || ctx.Err() != nil {
		return
	}
	// The write has committed. Notification failure must not turn a confirmed
	// save into failure; minute reconciliation repairs missed notifications.
	notifyCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := notify(notifyCtx); err != nil {
		logx.WithContext(notifyCtx).Errorw("job configuration notification failed", logx.Field("error", err))
	}
}
