package logic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxRoleListPageSize = 200
	maxRoleKeywordChars = 128
)

type ListRolesLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListRolesLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListRolesLogic {
	return &ListRolesLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListRolesLogic) ListRoles(in *system.ListRolesRequest) (*system.ListRolesResponse, error) {
	if in == nil || in.Page <= 0 || in.PageSize <= 0 ||
		in.PageSize > maxRoleListPageSize ||
		utf8.RuneCountInString(strings.TrimSpace(in.Keyword)) > maxRoleKeywordChars {
		return nil, status.Error(codes.InvalidArgument, "invalid role list request")
	}
	if in.Page-1 > int64(^uint64(0)>>1)/in.PageSize {
		return nil, status.Error(codes.InvalidArgument, "invalid role list request")
	}
	offset := (in.Page - 1) * in.PageSize
	if uint64(offset) > uint64(^uint(0)>>1) {
		return nil, status.Error(codes.InvalidArgument, "invalid role list request")
	}
	if l.svcCtx.RoleRepo == nil {
		return nil, errors.New("role repository is unavailable")
	}

	roles, total, err := l.svcCtx.RoleRepo.List(l.ctx, repository.RoleListQuery{
		Keyword: strings.TrimSpace(in.Keyword),
		Offset:  int(offset),
		Limit:   int(in.PageSize),
	})
	if err != nil {
		return nil, fmt.Errorf("list roles: %w", err)
	}
	items := make([]*system.RoleInfo, 0, len(roles))
	for _, role := range roles {
		items = append(items, toRoleInfo(role))
	}
	return &system.ListRolesResponse{Items: items, Total: total}, nil
}
