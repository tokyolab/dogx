package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	basemodel "github.com/tokyolab/dogx/pkg/model"
)

type jobServiceStub struct {
	called     string
	id         int64
	definition *model.JobDefinition
	status     basemodel.RecordStatus
	filter     repository.JobFilter
	manual     service.ManualTrigger
	detail     *repository.JobDetail
	items      []repository.JobListItem
	total      int64
	err        error
}

func (s *jobServiceStub) CreateJob(_ context.Context, in *model.JobDefinition) (int64, error) {
	s.called, s.definition = "CreateJob", in
	return 7, s.err
}
func (s *jobServiceStub) UpdateJob(_ context.Context, id int64, in *model.JobDefinition) error {
	s.called, s.id, s.definition = "UpdateJob", id, in
	return s.err
}
func (s *jobServiceStub) UpdateJobStatus(_ context.Context, id int64, value basemodel.RecordStatus) error {
	s.called, s.id, s.status = "UpdateJobStatus", id, value
	return s.err
}
func (s *jobServiceStub) DeleteJob(_ context.Context, id int64) error {
	s.called, s.id = "DeleteJob", id
	return s.err
}
func (s *jobServiceStub) GetJob(_ context.Context, id int64) (*repository.JobDetail, error) {
	s.called, s.id = "GetJob", id
	return s.detail, s.err
}
func (s *jobServiceStub) ListJobs(_ context.Context, filter repository.JobFilter) ([]repository.JobListItem, int64, error) {
	s.called, s.filter = "ListJobs", filter
	return s.items, s.total, s.err
}
func (s *jobServiceStub) TriggerManual(_ context.Context, in service.ManualTrigger) (int64, error) {
	s.called, s.manual = "TriggerManual", in
	return 9, s.err
}

var _ svc.JobService = (*jobServiceStub)(nil)
