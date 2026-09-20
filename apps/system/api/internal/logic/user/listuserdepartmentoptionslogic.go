// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package user

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/logic/department"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListUserDepartmentOptionsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListUserDepartmentOptionsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListUserDepartmentOptionsLogic {
	return &ListUserDepartmentOptionsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListUserDepartmentOptionsLogic) ListUserDepartmentOptions() (resp *types.DepartmentListResp, err error) {
	result, err := l.svcCtx.SystemRpc.ListDepartments(l.ctx, &systemclient.ListDepartmentsRequest{})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned no departments")
	}
	items := make([]types.DepartmentItem, 0, len(result.Items))
	for _, item := range result.Items {
		mapped, err := department.ToDepartmentItem(item)
		if err != nil {
			return nil, err
		}
		items = append(items, mapped)
	}
	return &types.DepartmentListResp{Items: items}, nil
}
