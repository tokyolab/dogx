package svc

import (
	"context"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	basemodel "github.com/tokyolab/dogx/pkg/model"
)

type JobService interface {
	CreateJob(context.Context, *model.JobDefinition) (int64, error)
	UpdateJob(context.Context, int64, *model.JobDefinition) error
	UpdateJobStatus(context.Context, int64, basemodel.RecordStatus) error
	DeleteJob(context.Context, int64) error
	GetJob(context.Context, int64) (*repository.JobDetail, error)
	ListJobs(context.Context, repository.JobFilter) ([]repository.JobListItem, int64, error)
	TriggerManual(context.Context, service.ManualTrigger) (int64, error)
}

func (s *ServiceContext) Now() time.Time {
	if s.Clock != nil {
		return s.Clock()
	}
	return time.Now()
}
