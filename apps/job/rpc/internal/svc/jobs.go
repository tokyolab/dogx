package svc

import (
	"context"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/trigger"
)

// ManualTrigger provides manual round creation to management RPCs, not Task
// execution. Implementations must support concurrent calls from RPC requests.
type ManualTrigger interface {
	// TriggerManual returns a committed Instance ID on success and replays
	// matching request identities using the original ID. Errors return a zero
	// ID, but a commit error does not prove that creation failed; retries must
	// reuse RequestID. Business rejections, context errors, and persistence
	// errors retain causes for errors.Is/As; Logic maps them to RPC status.
	// See trigger.Creator.TriggerManual for the complete trigger rules.
	TriggerManual(context.Context, trigger.ManualTrigger) (int64, error)
}

// Now returns process time for management use cases, using time.Now when Clock
// is nil. Clock replaces only the time source; it must not change while requests
// are running and does not represent the database server's clock.
func (s *ServiceContext) Now() time.Time {
	if s.Clock != nil {
		return s.Clock()
	}
	return time.Now()
}
