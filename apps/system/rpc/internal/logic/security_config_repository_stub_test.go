package logic

import (
	"context"
	"github.com/tokyolab/dogx/apps/system/internal/model"
)

type securityConfigRepositoryStub struct {
	cfg, updated *model.SecurityConfig
	err          error
}

func (s *securityConfigRepositoryStub) Get(context.Context) (*model.SecurityConfig, error) {
	return s.cfg, s.err
}
func (s *securityConfigRepositoryStub) UpdateLogin(_ context.Context, cfg *model.SecurityConfig) error {
	s.updated = cfg
	return s.err
}
