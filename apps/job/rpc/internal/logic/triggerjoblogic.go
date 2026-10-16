package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

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
	id, err := l.svcCtx.Jobs.TriggerManual(l.ctx, request)
	if err != nil {
		return nil, serviceError(err)
	}
	return &job.TriggerJobResponse{InstanceId: id}, nil
}
