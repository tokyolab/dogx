// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package loginlog

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ListLoginLogsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// List login audit records
func NewListLoginLogsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListLoginLogsLogic {
	return &ListLoginLogsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListLoginLogsLogic) ListLoginLogs(req *types.LoginLogListReq) (resp *types.LoginLogListResp, err error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid login log list request")
	}
	result, err := l.svcCtx.SystemRpc.ListLoginLogs(l.ctx, &systemclient.ListLoginLogsRequest{
		Page:     req.Page,
		PageSize: req.PageSize,
		Username: req.Username,
		Result:   req.Result,
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("system RPC returned an empty login log list")
	}
	items := make([]types.LoginLogItem, 0, len(result.Items))
	for _, item := range result.Items {
		if item == nil {
			return nil, errors.New("system RPC returned an invalid login log item")
		}
		items = append(items, types.LoginLogItem{
			Id:            item.Id,
			Username:      item.Username,
			Success:       item.Success,
			FailureReason: item.FailureReason,
			IpAddress:     item.IpAddress,
			UserAgent:     item.UserAgent,
			CreatedAt:     item.CreatedAt,
		})
	}
	return &types.LoginLogListResp{Items: items, Total: result.Total}, nil
}
