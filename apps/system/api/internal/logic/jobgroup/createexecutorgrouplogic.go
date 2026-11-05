// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package jobgroup

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type CreateExecutorGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCreateExecutorGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateExecutorGroupLogic {
	return &CreateExecutorGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *CreateExecutorGroupLogic) CreateExecutorGroup(req *types.CreateExecutorGroupReq) (resp *types.CreateExecutorGroupResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.CreateExecutorGroup(l.ctx, &jobclient.CreateExecutorGroupRequest{
		Name: req.Name, DiscoveryKey: req.DiscoveryKey,
	})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Id <= 0 {
		return nil, invalidResponse()
	}
	return &types.CreateExecutorGroupResp{Id: result.Id}, nil
}
