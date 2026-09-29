package logic

import (
	"context"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
)

type loginLogRepositoryStub struct {
	logs  []*model.LoginLog
	err   error
	query repository.LoginLogListQuery
	items []model.LoginLog
	total int64
	calls int
}

func (s *loginLogRepositoryStub) Create(_ context.Context, entry *model.LoginLog) error {
	copy := *entry
	s.logs = append(s.logs, &copy)
	return s.err
}

func (s *loginLogRepositoryStub) List(_ context.Context, query repository.LoginLogListQuery) ([]model.LoginLog, int64, error) {
	s.calls++
	s.query = query
	return s.items, s.total, s.err
}
