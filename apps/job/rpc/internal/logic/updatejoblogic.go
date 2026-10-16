package logic

import (
	"context"

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
	if err = l.svcCtx.Jobs.UpdateJob(l.ctx, in.Id, definition); err != nil {
		return nil, serviceError(err)
	}
	return &job.EmptyResponse{}, nil
}
