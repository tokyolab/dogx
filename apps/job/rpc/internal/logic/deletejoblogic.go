package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"
	basemodel "github.com/tokyolab/dogx/pkg/model"

	"github.com/zeromicro/go-zero/core/logx"
)

type DeleteJobLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewDeleteJobLogic(ctx context.Context, svcCtx *svc.ServiceContext) *DeleteJobLogic {
	return &DeleteJobLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *DeleteJobLogic) DeleteJob(in *job.DeleteJobRequest) (*job.EmptyResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidInput()
	}
	err := l.svcCtx.Transactions(l.ctx, func(r repository.Repositories) error {
		// Trigger creation holds this same row lock. Query active instances in
		// a new READ COMMITTED statement after the lock, to see its commit.
		definition, err := r.Jobs.Lock(l.ctx, in.Id)
		if err != nil {
			return err
		}
		if definition == nil {
			return missingJobRecord()
		}
		if definition.Status != basemodel.RecordStatusDisabled {
			return bizerror.New(subcode.JobStateNotAllowed, "请先停用任务再进行修改")
		}
		active, err := r.Instances.HasActive(l.ctx, in.Id)
		if err != nil {
			return err
		}
		if active {
			return bizerror.New(subcode.JobActiveInstances, "存在未结束的实例，不能删除任务")
		}
		return r.Jobs.Delete(l.ctx, in.Id)
	})
	if err != nil {
		return nil, jobManagementError("delete job", err)
	}
	notifyJobChanged(l.ctx, l.svcCtx.NotifyJobChanged)
	return &job.EmptyResponse{}, nil
}
