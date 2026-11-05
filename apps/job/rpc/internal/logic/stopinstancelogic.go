package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/job/internal/instancestop"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
)

type StopInstanceLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewStopInstanceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *StopInstanceLogic {
	return &StopInstanceLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *StopInstanceLogic) StopInstance(in *job.StopInstanceRequest) (*job.StopInstanceResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidInput()
	}
	decision, err := l.svcCtx.InstanceStopper.RequestManual(l.ctx, in.Id)
	if err != nil {
		if errors.Is(err, repository.ErrInstanceNotFound) {
			return nil, instanceNotFound()
		}
		return nil, rpcOperationError("stop job instance", err)
	}
	if decision.New && l.svcCtx.AfterInstanceStopped != nil {
		l.notifyStopped(decision)
	}
	return &job.StopInstanceResponse{
		InstanceId: decision.InstanceID, Status: int32(decision.Status), EndReason: decision.EndReason,
	}, nil
}

func (l *StopInstanceLogic) notifyStopped(decision instancestop.Decision) {
	// The decision is already committed. A failed local hint cannot undo it or
	// change the response into a retryable stop failure. Never log panic values.
	defer func() {
		if recover() != nil {
			l.Errorw("job instance stop notification panicked", logx.Field("instance_id", decision.InstanceID))
		}
	}()
	l.svcCtx.AfterInstanceStopped(decision)
}
