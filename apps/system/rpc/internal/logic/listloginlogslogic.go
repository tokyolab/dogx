package logic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ListLoginLogsLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewListLoginLogsLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ListLoginLogsLogic {
	return &ListLoginLogsLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ListLoginLogsLogic) ListLoginLogs(in *system.ListLoginLogsRequest) (*system.ListLoginLogsResponse, error) {
	if in == nil || in.Page <= 0 || in.PageSize <= 0 || in.PageSize > 200 ||
		utf8.RuneCountInString(in.Username) > 64 ||
		(in.Result != "" && in.Result != "success" && in.Result != "failure") {
		return nil, status.Error(codes.InvalidArgument, "invalid login log list request")
	}
	if in.Page-1 > int64(^uint64(0)>>1)/in.PageSize {
		return nil, status.Error(codes.InvalidArgument, "invalid login log pagination")
	}
	offset := (in.Page - 1) * in.PageSize
	if uint64(offset) > uint64(^uint(0)>>1) {
		return nil, status.Error(codes.InvalidArgument, "invalid login log pagination")
	}
	if l.svcCtx.LoginLogRepo == nil {
		return nil, errors.New("login log repository is unavailable")
	}
	query := repository.LoginLogListQuery{
		Username: strings.TrimSpace(in.Username),
		Offset:   int(offset),
		Limit:    int(in.PageSize),
	}
	if in.Result != "" {
		success := in.Result == "success"
		query.Success = &success
	}
	logs, total, err := l.svcCtx.LoginLogRepo.List(l.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list login logs: %w", err)
	}
	items := make([]*system.LoginLogInfo, 0, len(logs))
	for _, entry := range logs {
		items = append(items, &system.LoginLogInfo{
			Id:            entry.ID,
			Username:      entry.Username,
			Success:       entry.Success,
			FailureReason: entry.FailureReason,
			IpAddress:     entry.IPAddress,
			UserAgent:     entry.UserAgent,
			CreatedAt:     entry.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return &system.ListLoginLogsResponse{Items: items, Total: total}, nil
}
