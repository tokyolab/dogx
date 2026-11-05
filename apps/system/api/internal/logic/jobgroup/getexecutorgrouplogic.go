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

type GetExecutorGroupLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetExecutorGroupLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetExecutorGroupLogic {
	return &GetExecutorGroupLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetExecutorGroupLogic) GetExecutorGroup(req *types.IDReq) (resp *types.ExecutorGroupDetail, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	result, err := l.svcCtx.JobRpc.GetExecutorGroup(l.ctx, &jobclient.GetExecutorGroupRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Group == nil {
		return nil, invalidResponse()
	}
	group := result.Group
	return &types.ExecutorGroupDetail{
		Id: group.Id, Name: group.Name, DiscoveryKey: group.DiscoveryKey,
		Token: group.Token, CreatedAt: group.CreatedAt, UpdatedAt: group.UpdatedAt,
	}, nil
}
