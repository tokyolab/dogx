package scheduler

import (
	"context"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/service"
)

type triggerServiceStub struct {
	load    func(context.Context) ([]repository.Rule, error)
	due     func(context.Context, time.Time, int) ([]repository.DueJob, error)
	trigger func(context.Context, int64) (service.AutomaticResult, error)
}

func (s triggerServiceStub) LoadRules(ctx context.Context) ([]repository.Rule, error) {
	if s.load != nil {
		return s.load(ctx)
	}
	return nil, nil
}

func (s triggerServiceStub) ListDue(ctx context.Context, at time.Time, limit int) ([]repository.DueJob, error) {
	if s.due != nil {
		return s.due(ctx, at, limit)
	}
	return nil, nil
}

func (s triggerServiceStub) TriggerAutomatic(ctx context.Context, id int64) (service.AutomaticResult, error) {
	if s.trigger != nil {
		return s.trigger(ctx, id)
	}
	return service.AutomaticResult{}, nil
}
