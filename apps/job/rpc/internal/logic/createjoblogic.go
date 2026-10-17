package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/schedule"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	basemodel "github.com/tokyolab/dogx/pkg/model"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateJobLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateJobLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateJobLogic {
	return &CreateJobLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateJobLogic) CreateJob(in *job.CreateJobRequest) (*job.CreateJobResponse, error) {
	if in == nil || (in.Status != nil && !validStatus(*in.Status)) {
		return nil, invalidInput()
	}
	definition, err := normalizeInput(in.Config, true, l.svcCtx.Now())
	if err != nil {
		return nil, err
	}
	if in.Status != nil {
		definition.Status = basemodel.RecordStatus(*in.Status)
	}
	var id int64
	err = l.svcCtx.Transactions(l.ctx, func(r repository.Repositories) error {
		// Lock the group before creation so deletion cannot race this reference.
		if err := r.Groups.LockAvailable(l.ctx, definition.ExecutorGroupID); err != nil {
			return err
		}
		if definition.Status == basemodel.RecordStatusEnabled {
			// Derive the initial progress after acquiring the lock, not from the
			// earlier validation time, which may now be in the past.
			next, err := schedule.Next(definition.CronExpr, l.svcCtx.Now())
			if err != nil {
				return invalidInput()
			}
			definition.NextTriggerAt = &next
		}
		var err error
		id, err = r.Jobs.Create(l.ctx, definition)
		return err
	})
	if err != nil {
		return nil, jobManagementError("create job", err)
	}
	notifyJobChanged(l.ctx, l.svcCtx.NotifyJobChanged)
	return &job.CreateJobResponse{Id: id}, nil
}
