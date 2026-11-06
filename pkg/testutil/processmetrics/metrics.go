//go:build integration && e2e && load

// Package processmetrics samples the Go runtime of isolated load-test hosts.
// It does not collect application data, force garbage collection or open ports.
package processmetrics

import (
	"encoding/json"
	"io"
	"runtime"
	"sync"
	"time"
)

// Start writes one JSON line immediately and then every 500 milliseconds from
// one owned goroutine. The caller owns writer and must keep it open until the
// returned stop function finishes. stop is safe to call repeatedly or
// concurrently; it waits for sampling to end and returns the first write error.
// A blocked writer can delay stop. Samples include the sampler's own goroutine
// and report Go heap memory rather than operating-system resident memory.
func Start(writer io.Writer) func() error {
	stop, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var sampleErr error
	go func() {
		defer close(done)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		encoder := json.NewEncoder(writer)
		for {
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			sampleErr = encoder.Encode(struct {
				LoadResourceSample bool      `json:"load_resource_sample"`
				At                 time.Time `json:"at"`
				Goroutines         int       `json:"goroutines"`
				HeapAllocBytes     uint64    `json:"heap_alloc_bytes"`
				HeapSysBytes       uint64    `json:"heap_sys_bytes"`
			}{true, time.Now().UTC(), runtime.NumGoroutine(), memory.HeapAlloc, memory.HeapSys})
			if sampleErr != nil {
				return
			}
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	return func() error {
		once.Do(func() { close(stop) })
		<-done
		return sampleErr
	}
}
