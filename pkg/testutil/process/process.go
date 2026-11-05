// Package process sends termination signals to isolated test subprocesses.
// It does not own process startup, waiting, forced cleanup or application state.
package process

import (
	"os"
	"os/exec"
	"testing"
)

// Configure prepares an unstarted command for Signal. On Windows it replaces
// SysProcAttr with a hidden private console so a later CTRL_BREAK cannot reach
// the test runner or the user's terminal. On Unix it leaves command attributes
// unchanged. A nil or already-started command is a programming error.
func Configure(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process != nil {
		panic("process test helper requires an unstarted command")
	}
	configure(cmd)
}

// Signal sends CTRL_BREAK on Windows or SIGTERM on Unix to one live subprocess
// prepared by Configure. done must close only after the caller's Cmd.Wait has
// returned. It is checked before sending; a later exit can still make sending
// fail. Failure fails the current test without cancelling or killing the target.
// Success confirms signal submission, not the target's receipt, draining or exit.
// On Windows it also requires the attached sender to receive its own CTRL_BREAK.
//
// Windows starts the current test binary as a separate sender. Each consuming
// test package must declare TestProcessSignalSender and call RunSignalSender
// from it. The target PID travels over that child's stdin, not global arguments
// or environment. The caller retains ownership of process waiting and cleanup.
func Signal(t testing.TB, cmd *exec.Cmd, done <-chan struct{}) {
	t.Helper()
	if cmd == nil || cmd.Process == nil || done == nil {
		t.Fatal("a started test process and exit notification are required")
	}
	pid := cmd.Process.Pid
	if pid <= 0 || pid == os.Getpid() || pid == os.Getppid() {
		t.Fatal("refuse to signal an invalid or parent process ID")
	}
	select {
	case <-done:
		t.Fatal("cannot signal an exited test process")
	default:
	}
	signalProcess(t, cmd)
}

// RunSignalSender implements the fixed TestProcessSignalSender entry point in
// a consuming test package. Ordinary runs and Unix runs skip it. Only Signal's
// isolated Windows child may invoke its native-console work; it never runs from
// init, exits the process explicitly or modifies the test runner's console.
func RunSignalSender(t testing.TB) {
	t.Helper()
	runSignalSender(t)
}
