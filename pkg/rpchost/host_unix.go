//go:build linux || darwin || freebsd

package rpchost

import "github.com/zeromicro/go-zero/core/proc"

func terminationSignal() (<-chan struct{}, func(), func()) {
	select {
	case <-proc.Done():
		// A signal may have arrived before Run. A listener appended after the
		// one native notification would never run, so do not add or await one.
		return proc.Done(), nil, func() {}
	default:
	}
	// Registration holds go-zero's listener mutex, which native notification
	// also holds while callbacks run. A racing signal must not block Run before
	// it establishes its deadline. Both registration and the eventual callback
	// wait therefore remain behind Run's bounded wait, never on its control path.
	registered := make(chan func(), 1)
	go func() {
		registered <- proc.AddShutdownListener(func() {})
	}()
	wait := func() {
		(<-registered)()
	}
	return proc.Done(), wait, func() {}
}
