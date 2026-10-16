package logic

import (
	"context"
	"github.com/tokyolab/dogx/apps/job/internal/service"

	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetJobLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetJobLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetJobLogic {
	return &GetJobLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetJobLogic) GetJob(in *job.GetJobRequest) (*job.GetJobResponse, error) {
	if in == nil || in.Id <= 0 {
		return nil, invalidInput()
	}
	item, err := l.svcCtx.Jobs.GetJob(l.ctx, in.Id)
	if err != nil {
		return nil, serviceError(err)
	}
	if item == nil {
		return nil, serviceError(service.ErrNotFound)
	}
	return &job.GetJobResponse{Job: jobInfo(item)}, nil
}
