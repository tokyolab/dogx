package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateJobLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateJobLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateJobLogic {
	return &UpdateJobLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateJobLogic) UpdateJob(in *job.UpdateJobRequest) (*job.EmptyResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidInput()
	}
	definition, err := normalizeInput(in.Config, false, l.svcCtx.Now())
	if err != nil {
		return nil, err
	}
	err = l.svcCtx.Transactions(l.ctx, func(r repository.Repositories) error {
		// Lock the new target group before the conditional Job update; taking
		// the Job lock first would invert the group-deletion lock order.
		if err := r.Groups.LockAvailable(l.ctx, definition.ExecutorGroupID); err != nil {
			return err
		}
		return r.Jobs.Update(l.ctx, in.Id, definition)
	})
	if err != nil {
		return nil, jobManagementError("update job", err)
	}
	notifyJobChanged(l.ctx, l.svcCtx.NotifyJobChanged)
	return &job.EmptyResponse{}, nil
}
