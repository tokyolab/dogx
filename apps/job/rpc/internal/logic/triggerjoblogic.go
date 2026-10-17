package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/internal/trigger"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"

	"github.com/zeromicro/go-zero/core/logx"
)

type TriggerJobLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewTriggerJobLogic(ctx context.Context, svcCtx *svc.ServiceContext) *TriggerJobLogic {
	return &TriggerJobLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *TriggerJobLogic) TriggerJob(in *job.TriggerJobRequest) (*job.TriggerJobResponse, error) {
	request, err := normalizeTrigger(in)
	if err != nil {
		return nil, err
	}
	id, err := l.svcCtx.Trigger.TriggerManual(l.ctx, request)
	if err != nil {
		return nil, manualTriggerError(err)
	}
	return &job.TriggerJobResponse{InstanceId: id}, nil
}

func manualTriggerError(err error) error {
	switch {
	case errors.Is(err, trigger.ErrInvalidInput):
		return invalidInput()
	case errors.Is(err, repository.ErrJobNotFound):
		return bizerror.New(subcode.JobNotFound, "任务不存在")
	case errors.Is(err, repository.ErrExecutorGroupUnavailable):
		return bizerror.New(subcode.JobExecutorGroupUnavailable, "执行器组不可用")
	case errors.Is(err, trigger.ErrBlocked):
		return bizerror.New(subcode.JobTriggerBlocked, "存在未结束的实例，本次触发被阻塞")
	case errors.Is(err, trigger.ErrRequestConflict):
		return bizerror.New(subcode.JobTriggerRequestConflict, "请求标识已被用于不同的参数")
	case errors.Is(err, trigger.ErrCapacityBusy):
		return bizerror.New(subcode.JobTriggerCapacityBusy, "实例创建繁忙，请稍后重试")
	default:
		// An unresolved trigger identity or failed commit remains a technical
		// error, not a business rejection or a reason to invent a new request ID.
		return rpcOperationError("trigger job manually", err)
	}
}
