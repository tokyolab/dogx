//go:build integration && e2e && load

package main

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/tokyolab/dogx/pkg/testutil/processmetrics"
)

// TestJobLoadHostProcess runs the production Job host with runtime sampling.
// The parent provides an explicit isolated configuration using the same -f
// argument as the production executable; ordinary test invocations skip it.
func TestJobLoadHostProcess(t *testing.T) {
	var explicitConfig bool
	flag.Visit(func(value *flag.Flag) {
		if value.Name == "f" {
			explicitConfig = true
		}
	})
	if os.Getenv("DOGX_LOAD_RESOURCES") != "1" || !explicitConfig {
		t.Skip("Job load host is started by the end-to-end test")
	}
	stop := processmetrics.Start(os.Stdout)
	defer func() {
		if err := stop(); err != nil {
			t.Errorf("write Job load resource sample: %v", err)
		}
	}()
	if err := run(context.Background(), *configFile); err != nil {
		t.Fatalf("Job load host failed: %v", err)
	}
}
