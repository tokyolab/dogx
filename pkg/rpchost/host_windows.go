//go:build windows

package rpchost

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func terminationSignal() (<-chan struct{}, func(), func()) {
	// go-zero's Windows proc hooks are no-ops. Console interrupt and supported
	// close events therefore enter the host coordinator directly. Windows may
	// impose a shorter forced-exit deadline on window close or system shutdown.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	return ctx.Done(), nil, cancel
}
