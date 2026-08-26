package logic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxAPIKeywordBytes = 128
	maxAPIServiceBytes = 64
	maxAPIGroupBytes   = 64
)

type ListAPIsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListAPIsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListAPIsLogic {
	return &ListAPIsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListAPIsLogic) ListAPIs(in *system.ListAPIsRequest) (*system.ListAPIsResponse, error) {
	if in == nil ||
		len(strings.TrimSpace(in.Keyword)) > maxAPIKeywordBytes ||
		len(strings.TrimSpace(in.ServiceName)) > maxAPIServiceBytes ||
		len(strings.TrimSpace(in.ApiGroup)) > maxAPIGroupBytes {
		return nil, status.Error(codes.InvalidArgument, "invalid API list request")
	}
	if l.svcCtx.APIRepo == nil {
		return nil, errors.New("API repository is unavailable")
	}

	resources, err := l.svcCtx.APIRepo.List(l.ctx, repository.APIListQuery{
		Keyword:     strings.TrimSpace(in.Keyword),
		ServiceName: strings.TrimSpace(in.ServiceName),
		Group:       strings.TrimSpace(in.ApiGroup),
	})
	if err != nil {
		return nil, fmt.Errorf("list API resources: %w", err)
	}
	items := make([]*system.APIInfo, 0, len(resources))
	for _, resource := range resources {
		items = append(items, &system.APIInfo{
			Id:          resource.ID,
			ServiceName: resource.ServiceName,
			ApiGroup:    resource.Group,
			Name:        resource.Name,
			Path:        resource.Path,
			Method:      resource.Method,
			IsRequired:  resource.IsRequired,
			Status:      int32(resource.Status),
			Remark:      resource.Remark,
		})
	}
	return &system.ListAPIsResponse{Items: items}, nil
}
