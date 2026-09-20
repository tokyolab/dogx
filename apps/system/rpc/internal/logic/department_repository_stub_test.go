package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
)

type departmentRepositoryStub struct {
	departments []model.Department
	department  *model.Department
	listErr     error
	findID      int64
	findErr     error
	created     *model.Department
	createErr   error
	updateID    int64
	updated     *model.Department
	updateErr   error
	statusID    int64
	status      model.RecordStatus
	statusErr   error
	deleteID    int64
	deleteErr   error
}

func (s *departmentRepositoryStub) List(context.Context) ([]model.Department, error) {
	return s.departments, s.listErr
}

func (s *departmentRepositoryStub) FindByID(_ context.Context, id int64) (*model.Department, error) {
	s.findID = id
	return s.department, s.findErr
}

func (s *departmentRepositoryStub) Create(_ context.Context, department *model.Department) error {
	s.created = department
	if s.createErr == nil {
		department.ID = 42
	}
	return s.createErr
}

func (s *departmentRepositoryStub) Update(_ context.Context, id int64, department *model.Department) error {
	s.updateID, s.updated = id, department
	return s.updateErr
}

func (s *departmentRepositoryStub) UpdateStatus(_ context.Context, id int64, status model.RecordStatus) error {
	s.statusID, s.status = id, status
	return s.statusErr
}

func (s *departmentRepositoryStub) Delete(_ context.Context, id int64) error {
	s.deleteID = id
	return s.deleteErr
}

var _ repository.DepartmentRepository = (*departmentRepositoryStub)(nil)
