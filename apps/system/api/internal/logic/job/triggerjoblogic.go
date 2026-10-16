// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package job

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/authctx"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zeromicro/go-zero/core/logx"
)

type TriggerJobLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewTriggerJobLogic(ctx context.Context, svcCtx *svc.ServiceContext) *TriggerJobLogic {
	return &TriggerJobLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *TriggerJobLogic) TriggerJob(req *types.TriggerJobReq) (resp *types.TriggerJobResp, err error) {
	if req == nil {
		return nil, invalidRequest()
	}
	identity, err := authctx.FromContext(l.ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid authenticated identity")
	}
	result, err := l.svcCtx.JobRpc.TriggerJob(l.ctx, &jobclient.TriggerJobRequest{Id: req.Id, RequestId: req.RequestId, ParamsOverridden: req.ParamsOverridden, Params: req.Params, OperatorId: identity.UserID})
	if err != nil {
		return nil, err
	}
	if result == nil || result.InstanceId <= 0 {
		return nil, invalidResponse()
	}
	return &types.TriggerJobResp{InstanceId: result.InstanceId}, nil
}
