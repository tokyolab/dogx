package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
)

type menuRepositoryStub struct {
	menus  []model.Menu
	menu   *model.Menu
	id     int64
	status model.RecordStatus
	writes int
	reads  int
	err    error
}

func (s *menuRepositoryStub) List(context.Context) ([]model.Menu, error) {
	s.reads++
	return s.menus, s.err
}
func (s *menuRepositoryStub) FindByID(_ context.Context, id int64) (*model.Menu, error) {
	s.id = id
	return s.menu, s.err
}
func (s *menuRepositoryStub) Create(_ context.Context, m *model.Menu) error {
	s.writes++
	s.menu = m
	m.ID = 42
	return s.err
}
func (s *menuRepositoryStub) Update(_ context.Context, id int64, m *model.Menu) error {
	s.writes++
	s.id = id
	s.menu = m
	return s.err
}
func (s *menuRepositoryStub) UpdateStatus(_ context.Context, id int64, status model.RecordStatus) error {
	s.writes++
	s.id = id
	s.status = status
	return s.err
}
func (s *menuRepositoryStub) Delete(_ context.Context, id int64) error {
	s.writes++
	s.id = id
	return s.err
}

var _ repository.MenuRepository = (*menuRepositoryStub)(nil)
