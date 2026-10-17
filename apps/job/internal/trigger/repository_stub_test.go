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
func (r *repositoryStub) ListWaitingIDs(context.Context, int64, int) ([]int64, error) {
	return nil, r.call("waiting")
}
func (r *repositoryStub) LoadWaiting(context.Context, []int64) ([]repository.WaitingInstance, error) {
	return nil, r.call("load_waiting")
}
