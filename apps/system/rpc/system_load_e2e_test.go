//go:build integration && e2e && load

package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/tokyolab/dogx/pkg/testutil/processmetrics"
)

// TestMain adds runtime sampling only to the isolated execution load host.
// TestJobExecutorHostProcess still owns the production run call and business
// fixture; other tests built with the load tag retain their normal behavior.
func TestMain(m *testing.M) {
	if os.Getenv("DOGX_LOAD_RESOURCES") != "1" || os.Getenv("DOGX_E2E_SYSTEM_CONFIG") == "" {
		os.Exit(m.Run())
	}
	stop := processmetrics.Start(os.Stdout)
	code := m.Run()
	if err := stop(); err != nil {
		fmt.Fprintf(os.Stderr, "write System load resource sample: %v\n", err)
		code = 1
	}
	os.Exit(code)
}
