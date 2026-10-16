package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

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
	if err := l.svcCtx.Jobs.DeleteJob(l.ctx, in.Id); err != nil {
		return nil, serviceError(err)
	}
	return &job.EmptyResponse{}, nil
}
