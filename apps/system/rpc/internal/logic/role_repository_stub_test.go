package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
)

type roleRepositoryStub struct {
	listQuery repository.RoleListQuery
	roles     []model.Role
	total     int64
	listErr   error
	role      *model.Role
	findID    int64
	findErr   error
	created   *model.Role
	createErr error
	updateID  int64
	update    repository.RoleUpdate
	updateErr error
}

func (s *roleRepositoryStub) ListEnabledRoleIDs(context.Context, int64) ([]int64, error) {
	return nil, nil
}

func (s *roleRepositoryStub) IsSuperAdmin(context.Context, int64) (bool, error) {
	return false, nil
}

func (s *roleRepositoryStub) List(
	_ context.Context,
	query repository.RoleListQuery,
) ([]model.Role, int64, error) {
	s.listQuery = query
	return s.roles, s.total, s.listErr
}

func (s *roleRepositoryStub) FindByID(_ context.Context, id int64) (*model.Role, error) {
	s.findID = id
	return s.role, s.findErr
}

func (s *roleRepositoryStub) Create(_ context.Context, role *model.Role) error {
	s.created = role
	if s.createErr == nil {
		role.ID = 41
	}
	return s.createErr
}

func (s *roleRepositoryStub) Update(
	_ context.Context,
	id int64,
	update repository.RoleUpdate,
) error {
	s.updateID = id
	s.update = update
	return s.updateErr
}

var _ repository.RoleRepository = (*roleRepositoryStub)(nil)
