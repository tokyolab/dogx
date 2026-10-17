package svc

import (
	"context"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/trigger"
)

type ManualTrigger interface {
	TriggerManual(context.Context, trigger.ManualTrigger) (int64, error)
}

func (s *ServiceContext) Now() time.Time {
	if s.Clock != nil {
		return s.Clock()
	}
	return time.Now()
}
