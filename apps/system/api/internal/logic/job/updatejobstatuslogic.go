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

type UpdateJobStatusLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewUpdateJobStatusLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateJobStatusLogic {
	return &UpdateJobStatusLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *UpdateJobStatusLogic) UpdateJobStatus(req *types.UpdateJobStatusReq) (resp *types.EmptyResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.UpdateJobStatus(l.ctx, &jobclient.UpdateJobStatusRequest{Id: req.Id, Status: req.Status})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, invalidResponse()
	}
	return &types.EmptyResp{}, nil
}
