// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package job

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type PreviewJobCronLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewPreviewJobCronLogic(ctx context.Context, svcCtx *svc.ServiceContext) *PreviewJobCronLogic {
	return &PreviewJobCronLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *PreviewJobCronLogic) PreviewJobCron(req *types.PreviewJobCronReq) (resp *types.PreviewJobCronResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.PreviewJobCron(l.ctx, &jobclient.PreviewJobCronRequest{CronExpr: req.CronExpr})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidResponse()
	}
	return &types.PreviewJobCronResp{Times: result.Times}, nil
}
