package logic

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
)

type jobRepositoryStub struct {
	definition        *model.JobDefinition
	detail            *repository.JobDetail
	items             []repository.JobListItem
	total             int64
	saved             *model.JobDefinition
	filter            repository.JobFilter
	id                int64
	enabled           *time.Time
	available, active bool
	calls             []string
	failures          map[string]error
	err               error
}

func (s *jobRepositoryStub) call(name string) error {
	s.calls = append(s.calls, name)
	if err := s.failures[name]; err != nil {
		return err
	}
	return s.err
}
func (s *jobRepositoryStub) Create(_ context.Context, in *model.JobDefinition) (int64, error) {
	s.saved = in
	return 7, s.call("create")
}
func (s *jobRepositoryStub) Update(_ context.Context, id int64, in *model.JobDefinition) error {
	s.id, s.saved = id, in
	return s.call("update")
}
func (s *jobRepositoryStub) Enable(_ context.Context, id int64, next time.Time) error {
	s.id, s.enabled = id, &next
	return s.call("enable")
}
func (s *jobRepositoryStub) Disable(_ context.Context, id int64) error {
	s.id = id
	return s.call("disable")
}
func (s *jobRepositoryStub) Delete(_ context.Context, id int64) error {
	s.id = id
	return s.call("delete")
}
func (s *jobRepositoryStub) Get(_ context.Context, id int64) (*repository.JobDetail, error) {
	s.id = id
	return s.detail, s.call("get")
}
func (s *jobRepositoryStub) List(_ context.Context, filter repository.JobFilter) ([]repository.JobListItem, int64, error) {
	s.filter = filter
	return s.items, s.total, s.call("list")
}
func (s *jobRepositoryStub) Lock(context.Context, int64) (*model.JobDefinition, error) {
	return s.definition, s.call("lock")
}
func (s *jobRepositoryStub) Advance(context.Context, int64, time.Time) error {
	return s.call("advance")
}
func (s *jobRepositoryStub) LoadRules(context.Context) ([]repository.Rule, error) {
	return nil, s.call("rules")
}
func (s *jobRepositoryStub) ListDue(context.Context, time.Time, int) ([]repository.DueJob, error) {
	return nil, s.call("due")
}
func (s *jobRepositoryStub) LockAvailable(context.Context, int64) error {
	if err := s.call("group_lock"); err != nil {
		return err
	}
	if !s.available {
		return repository.ErrExecutorGroupUnavailable
	}
	return nil
}
func (s *jobRepositoryStub) Available(context.Context, int64) (bool, error) {
	return s.available, s.call("available")
}
func (s *jobRepositoryStub) HasActive(context.Context, int64) (bool, error) {
	return s.active, s.call("active")
}
func (s *jobRepositoryStub) FindManual(context.Context, int64, uuid.UUID) (*repository.ManualIdentity, error) {
	return nil, s.call("find")
}
func (s *jobRepositoryStub) Insert(context.Context, *model.JobInstance) (int64, bool, error) {
	return 0, false, s.call("insert")
}
func (s *jobRepositoryStub) ListWaitingIDs(context.Context, int64, int) ([]int64, error) {
	return nil, s.call("waiting")
}
func (s *jobRepositoryStub) LoadWaiting(context.Context, []int64) ([]repository.WaitingInstance, error) {
	return nil, s.call("load_waiting")
}

func managementContext(r *jobRepositoryStub) *svc.ServiceContext {
	return &svc.ServiceContext{
		JobRepo: r, Clock: testNow,
		Transactions: func(_ context.Context, fn func(repository.Repositories) error) error {
			return fn(repository.Repositories{Jobs: r, Groups: r, Instances: r})
		},
	}
}

var (
	_ repository.JobDefinitionRepository = (*jobRepositoryStub)(nil)
	_ repository.ExecutorGroupRepository = (*jobRepositoryStub)(nil)
	_ repository.JobInstanceRepository   = (*jobRepositoryStub)(nil)
)
