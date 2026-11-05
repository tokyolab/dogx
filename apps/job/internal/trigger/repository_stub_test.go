package trigger

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

type repositoryStub struct {
	job                        *model.JobDefinition
	available, active, created bool
	identities                 []*repository.ManualIdentity
	finds                      int
	inserted                   *model.JobInstance
	saved                      *model.JobDefinition
	advanced, enabled          *time.Time
	calls                      []string
	failures                   map[string]error
}

func (*repositoryStub) LockForStop(context.Context, int64, bool) (*repository.StoppingInstance, error) {
	panic("unexpected LockForStop")
}
func (*repositoryStub) RecordStop(context.Context, int64, model.InstanceStatus, string, string, time.Time) (bool, error) {
	panic("unexpected RecordStop")
}
func (*repositoryStub) FinishStop(context.Context, int64, string) (bool, error) {
	panic("unexpected FinishStop")
}

func (*repositoryStub) StoppingUpperID(context.Context) (int64, error) {
	panic("unexpected StoppingUpperID")
}

func (*repositoryStub) ListRunningCandidates(context.Context, time.Time, *repository.RunningCandidate, int) ([]repository.RunningCandidate, error) {
	panic("unexpected ListRunningCandidates")
}
func (*repositoryStub) ListStoppingCandidates(context.Context, int64, int64, int) ([]int64, error) {
	panic("unexpected ListStoppingCandidates")
}

func (*repositoryStub) Exists(context.Context, int64) (bool, error) {
	panic("unexpected Exists")
}
func (*repositoryStub) FindCompletion(context.Context, int64) (*repository.InstanceCompletion, error) {
	panic("unexpected FindCompletion")
}
func (*repositoryStub) FindByID(context.Context, int64) (*model.JobInstance, error) {
	panic("unexpected FindByID")
}
func (*repositoryStub) Complete(context.Context, int64, model.InstanceStatus, *string) (bool, error) {
	panic("unexpected Complete")
}

func (r *repositoryStub) call(name string) error {
	r.calls = append(r.calls, name)
	return r.failures[name]
}
func (r *repositoryStub) List(context.Context, repository.JobFilter) ([]repository.JobListItem, int64, error) {
	return []repository.JobListItem{}, 0, r.call("list")
}
func (r *repositoryStub) Get(context.Context, int64) (*repository.JobDetail, error) {
	return &repository.JobDetail{JobDefinition: *r.job}, r.call("get")
}
func (*repositoryStub) HasGroupReferences(context.Context, int64) (bool, error) {
	panic("unexpected executor group reference check")
}
func (r *repositoryStub) Lock(context.Context, int64) (*model.JobDefinition, error) {
	return r.job, r.call("lock")
}
func (r *repositoryStub) Create(_ context.Context, j *model.JobDefinition) (int64, error) {
	r.saved = j
	return 99, r.call("create")
}
func (r *repositoryStub) Update(_ context.Context, _ int64, j *model.JobDefinition) error {
	r.saved = j
	return r.call("update")
}
func (r *repositoryStub) Enable(_ context.Context, _ int64, next time.Time) error {
	r.enabled = &next
	return r.call("enable")
}
func (r *repositoryStub) Disable(context.Context, int64) error { return r.call("disable") }
func (r *repositoryStub) Delete(context.Context, int64) error  { return r.call("delete") }
func (r *repositoryStub) Advance(_ context.Context, _ int64, next time.Time) error {
	r.advanced = &next
	return r.call("advance")
}
func (r *repositoryStub) LoadRules(context.Context) ([]repository.Rule, error) {
	return []repository.Rule{{ID: r.job.ID, CronExpr: r.job.CronExpr}}, r.call("rules")
}
func (r *repositoryStub) ListDue(context.Context, time.Time, int) ([]repository.DueJob, error) {
	return []repository.DueJob{}, r.call("due")
}
func (r *repositoryStub) LockAvailable(context.Context, int64) error {
	if err := r.call("group_lock"); err != nil {
		return err
	}
	if !r.available {
		return repository.ErrExecutorGroupUnavailable
	}
	return nil
}
func (r *repositoryStub) Available(context.Context, int64) (bool, error) {
	return r.available, r.call("available")
}
func (r *repositoryStub) HasActive(context.Context, int64) (bool, error) {
	return r.active, r.call("active")
}
func (*repositoryStub) HasActiveForGroup(context.Context, int64) (bool, error) {
	panic("unexpected executor group active instance check")
}
func (r *repositoryStub) FindManual(context.Context, int64, uuid.UUID) (*repository.ManualIdentity, error) {
	var identity *repository.ManualIdentity
	if r.finds < len(r.identities) {
		identity = r.identities[r.finds]
	}
	r.finds++
	return identity, r.call("find")
}
func (r *repositoryStub) Insert(_ context.Context, i *model.JobInstance) (int64, bool, error) {
	r.inserted = i
	return 99, r.created, r.call("insert")
}
func (r *repositoryStub) MaxWaitingID(context.Context) (int64, error) {
	return 0, r.call("waiting_upper")
}
func (r *repositoryStub) ListWaitingIDs(context.Context, int64, int64, int) ([]int64, error) {
	return nil, r.call("waiting")
}
func (r *repositoryStub) LoadWaiting(context.Context, []int64) ([]repository.WaitingInstance, error) {
	return nil, r.call("load_waiting")
}
func (*repositoryStub) LockForExecution(context.Context, int64) (*repository.ExecutionInstance, error) {
	panic("unexpected execution instance lock")
}
func (*repositoryStub) DatabaseTime(context.Context) (time.Time, error) {
	panic("unexpected execution database time query")
}
func (*repositoryStub) StartExecution(context.Context, int64, time.Time, time.Time) (bool, error) {
	panic("unexpected execution instance start")
}

// groupRepositoryStub shares the fixture's call ordering and failures while
// keeping group-specific CRUD signatures separate from the Job repository.
type groupRepositoryStub struct{ *repositoryStub }

func (*groupRepositoryStub) Create(context.Context, *model.ExecutorGroup) (int64, error) {
	panic("unexpected executor group creation")
}
func (*groupRepositoryStub) FindByID(context.Context, int64) (*model.ExecutorGroup, error) {
	panic("unexpected executor group detail")
}
func (*groupRepositoryStub) Delete(context.Context, int64) error {
	panic("unexpected executor group deletion")
}
func (*groupRepositoryStub) List(context.Context, repository.ExecutorGroupFilter) ([]repository.ExecutorGroupListItem, int64, error) {
	panic("unexpected executor group list")
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
