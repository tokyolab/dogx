package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/repository"
)

type roleMenuRepositoryStub struct {
	ids, roles, saved []int64
	roleID            int64
	reads, writes     int
	err               error
}

func (s *roleMenuRepositoryStub) ListMenuIDs(_ context.Context, roles []int64) ([]int64, error) {
	s.reads++
	s.roles = roles
	return s.ids, s.err
}
func (s *roleMenuRepositoryStub) Replace(_ context.Context, id int64, ids []int64) error {
	s.writes++
	s.roleID = id
	s.saved = ids
	return s.err
}

var _ repository.RoleMenuRepository = (*roleMenuRepositoryStub)(nil)
