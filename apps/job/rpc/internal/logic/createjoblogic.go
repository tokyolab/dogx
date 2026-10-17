package logic

import (
	"context"

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
	id, err := l.svcCtx.Jobs.CreateJob(l.ctx, definition)
	if err != nil {
		return nil, serviceError(err)
	}
	return &job.CreateJobResponse{Id: id}, nil
}
