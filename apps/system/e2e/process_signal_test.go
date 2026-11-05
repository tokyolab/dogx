//go:build integration && e2e

package e2e

import (
	"os/exec"
	"testing"
	"time"

	processutil "github.com/tokyolab/dogx/pkg/testutil/process"
)

func configureE2ESignalProcess(cmd *exec.Cmd) {
	processutil.Configure(cmd)
}

func signalE2EProcess(t testing.TB, process *runningProcess) {
	t.Helper()
	if process == nil {
		t.Fatal("a started end-to-end process is required")
	}
	processutil.Signal(t, process.cmd, process.done)
}

// TestProcessSignalSender is the fixed entry point for the isolated Windows
// sender. Normal test runs skip it; the target PID is supplied over child stdin.
func TestProcessSignalSender(t *testing.T) {
	processutil.RunSignalSender(t)
}

// waitE2EProcessExit returns the actual process exit error. A deadline failure
// fails the test without killing the process; its existing cleanup still owns
// forced teardown. In particular, forced termination never counts as success.
func waitE2EProcessExit(t testing.TB, process *runningProcess, timeout time.Duration) error {
	t.Helper()
	if process == nil || timeout <= 0 {
		t.Fatal("a process and positive exit timeout are required")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-process.done:
		process.mu.Lock()
		defer process.mu.Unlock()
		return process.err
	case <-timer.C:
		t.Fatalf("%s did not exit within %s after its termination signal", process.name, timeout)
		return nil
	}
}
