package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
)

type ListUsersLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListUsersLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListUsersLogic {
	return &ListUsersLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListUsersLogic) ListUsers(in *system.ListUsersRequest) (*system.ListUsersResponse, error) {
	if in == nil || (in.Status != nil && !validRecordStatus(*in.Status)) {
		return nil, status.Error(codes.InvalidArgument, "invalid user list request")
	}
	offset, err := userPageOffset(in.Page, in.PageSize, in.Keyword)
	if err != nil {
		return nil, err
	}
	query := repository.UserListQuery{Keyword: in.Keyword, Offset: offset, Limit: int(in.PageSize)}
	if in.Status != nil {
		value := model.RecordStatus(*in.Status)
		query.Status = &value
	}
	records, total, err := l.svcCtx.UserRepo.List(l.ctx, query)
	if err != nil {
		return nil, userManagementError(err)
	}
	items := make([]*system.UserInfo, 0, len(records))
	for _, record := range records {
		items = append(items, toUserInfo(record))
	}
	return &system.ListUsersResponse{Items: items, Total: total}, nil
}
