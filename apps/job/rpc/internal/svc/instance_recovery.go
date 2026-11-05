package svc

import (
	"context"
	"errors"
	"time"
)

// runInstanceRecovery drives both discovery branches with one elected-term
// clock. Each branch owns its cursor and budgets and runs an immediate pass.
// Unbuffered, nonblocking ticks let an available branch proceed even if the
// other is still waiting for storage cleanup; busy branches skip that tick.
// Neither branch can overlap its own passes or accumulate catch-up work.
//
// Term cancellation stops discovery, not process-owned work already admitted.
// An unexpected branch exit cancels its sibling. This function waits for both
// actual returns before releasing term resources or permitting local reelection.
func runInstanceRecovery(ctx context.Context, interval time.Duration,
	running, stopping func(context.Context, <-chan struct{}) error,
) error {
	if interval <= 0 || running == nil || stopping == nil {
		return errors.New("instance recovery requires a positive interval and both branches")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	runningTicks, stoppingTicks := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 2)
	go func() { finished <- running(ctx, runningTicks) }()
	go func() { finished <- stopping(ctx, stoppingTicks) }()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), <-finished, <-finished)
		case err := <-finished:
			if err == nil {
				err = errors.New("instance discovery exited before term cancellation")
			}
			cancel()
			return errors.Join(err, <-finished)
		case <-ticker.C:
			select {
			case runningTicks <- struct{}{}:
			default:
			}
			select {
			case stoppingTicks <- struct{}{}:
			default:
			}
		}
	}
}
