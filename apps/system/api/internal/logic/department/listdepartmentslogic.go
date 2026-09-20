// Code scaffolded by goctl. Safe to edit.
// goctl 1.10.2

package department

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ListDepartmentsLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewListDepartmentsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListDepartmentsLogic {
	return &ListDepartmentsLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *ListDepartmentsLogic) ListDepartments() (resp *types.DepartmentListResp, err error) {
	result, err := l.svcCtx.SystemRpc.ListDepartments(l.ctx, &systemclient.ListDepartmentsRequest{})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, status.Error(codes.Internal, "system RPC returned no department list")
	}
	items := make([]types.DepartmentItem, 0, len(result.GetItems()))
	for _, item := range result.GetItems() {
		mapped, mapErr := ToDepartmentItem(item)
		if mapErr != nil {
			return nil, mapErr
		}
		items = append(items, mapped)
	}
	return &types.DepartmentListResp{Items: items}, nil
}
