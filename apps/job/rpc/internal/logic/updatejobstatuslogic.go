package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/schedule"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"
	basemodel "github.com/tokyolab/dogx/pkg/model"

	"github.com/zeromicro/go-zero/core/logx"
)

type UpdateJobStatusLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateJobStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateJobStatusLogic {
	return &UpdateJobStatusLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateJobStatusLogic) UpdateJobStatus(in *job.UpdateJobStatusRequest) (*job.EmptyResponse, error) {
	if in == nil || in.Id <= 0 || in.Status == nil || !validStatus(*in.Status) {
		return nil, invalidInput()
	}
	var err error
	if basemodel.RecordStatus(*in.Status) == basemodel.RecordStatusDisabled {
		err = l.svcCtx.JobRepo.Disable(l.ctx, in.Id)
	} else {
		err = l.svcCtx.Transactions(l.ctx, func(r repository.Repositories) error {
			// Serialize enabling and trigger progress on this Job. An idempotent
			// enable must not reset a previously established next trigger time.
			definition, err := r.Jobs.Lock(l.ctx, in.Id)
			if err != nil {
				return err
			}
			if definition == nil {
				return missingJobRecord()
			}
			if definition.Status == basemodel.RecordStatusEnabled {
				return nil
			}
			// Do not lock the group after the Job: group mutation uses the
			// opposite order. The existing Job reference protects group deletion.
			available, err := r.Groups.Available(l.ctx, definition.ExecutorGroupID)
			if err != nil {
				return err
			}
			if !available {
				return bizerror.New(subcode.JobExecutorGroupUnavailable, "执行器组不可用")
			}
			if err := validateStoredDefinition(definition); err != nil {
				return err
			}
			next, err := schedule.Next(definition.CronExpr, l.svcCtx.Now())
			if err != nil {
				return invalidInput()
			}
			return r.Jobs.Enable(l.ctx, in.Id, next)
		})
	}
	if err != nil {
		return nil, jobManagementError("update job status", err)
	}
	notifyJobChanged(l.ctx, l.svcCtx.NotifyJobChanged)
	return &job.EmptyResponse{}, nil
}
