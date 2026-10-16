package logic

import (
	"context"
	"github.com/tokyolab/dogx/apps/job/internal/schedule"
	"time"

	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"

	"github.com/zeromicro/go-zero/core/logx"
)

type PreviewJobCronLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewPreviewJobCronLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PreviewJobCronLogic {
	return &PreviewJobCronLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *PreviewJobCronLogic) PreviewJobCron(in *job.PreviewJobCronRequest) (*job.PreviewJobCronResponse, error) {
	if in == nil {
		return nil, invalidInput()
	}
	expression, err := normalizeText(in.CronExpr, true, 128)
	if err != nil {
		return nil, err
	}
	times, err := schedule.Preview(expression, l.svcCtx.Now())
	if err != nil {
		return nil, invalidInput()
	}
	result := make([]string, 0, len(times))
	for _, value := range times {
		result = append(result, value.Format(time.RFC3339Nano))
	}
	return &job.PreviewJobCronResponse{Times: result}, nil
}
