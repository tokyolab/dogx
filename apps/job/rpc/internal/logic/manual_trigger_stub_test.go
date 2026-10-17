package logic

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/internal/trigger"
)

type manualTriggerStub struct {
	called bool
	input  trigger.ManualTrigger
	err    error
}

func (s *manualTriggerStub) TriggerManual(_ context.Context, input trigger.ManualTrigger) (int64, error) {
	s.called, s.input = true, input
	return 9, s.err
}
