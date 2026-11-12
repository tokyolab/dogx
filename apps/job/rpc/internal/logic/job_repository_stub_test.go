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
	group             *model.ExecutorGroup
	groupSaved        *model.ExecutorGroup
	groupItems        []repository.ExecutorGroupListItem
	groupFilter       repository.ExecutorGroupFilter
	groupID           int64
	groupReferences   bool
	groupActive       bool
}

func (*jobRepositoryStub) Exists(context.Context, int64) (bool, error) {
	panic("unexpected Exists")
}
func (*jobRepositoryStub) FindCompletion(context.Context, int64) (*repository.InstanceCompletion, error) {
	panic("unexpected FindCompletion")
}
func (*jobRepositoryStub) FindByID(context.Context, int64) (*model.JobInstance, error) {
	panic("unexpected FindByID")
}
func (*jobRepositoryStub) Complete(context.Context, int64, model.InstanceStatus, *string) (bool, error) {
	panic("unexpected Complete")
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
func (s *jobRepositoryStub) HasGroupReferences(context.Context, int64) (bool, error) {
	return s.groupReferences, s.call("group_references")
}
func (s *jobRepositoryStub) HasActiveForGroup(context.Context, int64) (bool, error) {
	return s.groupActive, s.call("group_active")
}
func (s *jobRepositoryStub) FindManual(context.Context, int64, uuid.UUID) (*repository.ManualIdentity, error) {
	return nil, s.call("find")
}
func (s *jobRepositoryStub) Insert(context.Context, *model.JobInstance) (int64, bool, error) {
	return 0, false, s.call("insert")
}
func (s *jobRepositoryStub) MaxWaitingID(context.Context) (int64, error) {
	return 0, s.call("waiting_upper")
}
func (s *jobRepositoryStub) ListWaitingIDs(context.Context, int64, int64, int) ([]int64, error) {
	return nil, s.call("waiting")
}
func (s *jobRepositoryStub) LoadWaiting(context.Context, []int64) ([]repository.WaitingInstance, error) {
	return nil, s.call("load_waiting")
}
func (*jobRepositoryStub) LockForStop(context.Context, int64, bool) (*repository.StoppingInstance, error) {
	panic("unexpected LockForStop")
}
func (*jobRepositoryStub) RecordStop(context.Context, int64, model.InstanceStatus, string, string, time.Time) (bool, error) {
	panic("unexpected RecordStop")
}
func (*jobRepositoryStub) FinishStop(context.Context, int64, string) (bool, error) {
	panic("unexpected FinishStop")
}

func (*jobRepositoryStub) StoppingUpperID(context.Context) (int64, error) {
	panic("unexpected StoppingUpperID")
}

func (*jobRepositoryStub) ListRunningCandidates(context.Context, time.Time, *repository.RunningCandidate, int) ([]repository.RunningCandidate, error) {
	panic("unexpected ListRunningCandidates")
}
func (*jobRepositoryStub) ListStoppingCandidates(context.Context, int64, int64, int) ([]int64, error) {
	panic("unexpected ListStoppingCandidates")
}

func (*jobRepositoryStub) LockForExecution(context.Context, int64) (*repository.ExecutionInstance, error) {
	panic("unexpected execution instance lock")
}
func (*jobRepositoryStub) DatabaseTime(context.Context) (time.Time, error) {
	panic("unexpected execution database time query")
}
func (*jobRepositoryStub) StartExecution(context.Context, int64, time.Time, time.Time) (bool, error) {
	panic("unexpected execution instance start")
}

func managementContext(r *jobRepositoryStub) *svc.ServiceContext {
	return &svc.ServiceContext{
		JobRepo: r, GroupRepo: &groupRepositoryStub{r}, Clock: testNow,
		Transactions: func(_ context.Context, fn func(repository.Repositories) error) error {
			return fn(repository.Repositories{Jobs: r, Groups: &groupRepositoryStub{r}, Instances: &managementInstanceStub{r}})
		},
	}
}

var (
	_ repository.JobDefinitionRepository = (*jobRepositoryStub)(nil)
	_ repository.ExecutorGroupRepository = (*groupRepositoryStub)(nil)
	_ repository.JobInstanceRepository   = (*managementInstanceStub)(nil)
)

// managementInstanceStub shares the fixture's call ordering while keeping the
// Instance list signature separate from Job management's List method.
type managementInstanceStub struct{ *jobRepositoryStub }

func (*managementInstanceStub) List(context.Context, repository.InstanceFilter) ([]repository.InstanceListItem, int64, error) {
	panic("unexpected instance history query")
}

// groupRepositoryStub shares the fixture's call ordering and failures while
// keeping group-specific CRUD signatures separate from the Job repository.
type groupRepositoryStub struct{ *jobRepositoryStub }

func (s *groupRepositoryStub) Create(_ context.Context, group *model.ExecutorGroup) (int64, error) {
	s.groupSaved = group
	return s.groupID, s.call("group_create")
}
func (s *groupRepositoryStub) FindByID(_ context.Context, id int64) (*model.ExecutorGroup, error) {
	s.id = id
	return s.group, s.call("group_get")
}
func (s *groupRepositoryStub) List(_ context.Context, filter repository.ExecutorGroupFilter) ([]repository.ExecutorGroupListItem, int64, error) {
	s.groupFilter = filter
	return s.groupItems, s.total, s.call("group_list")
}
func (s *groupRepositoryStub) Delete(_ context.Context, id int64) error {
	s.id = id
	return s.call("group_delete")
}
func (*groupRepositoryStub) LoadExecutionGroups(context.Context) ([]repository.ExecutorGroupConfig, error) {
	panic("unexpected executor group snapshot")
}
func (*groupRepositoryStub) FindExecutionGroupByID(context.Context, int64) (*repository.ExecutorGroupConfig, error) {
	panic("unexpected executor group configuration by ID")
}
func (*groupRepositoryStub) FindExecutionGroupByName(context.Context, string) (*repository.ExecutorGroupConfig, error) {
	panic("unexpected executor group configuration by name")
}
