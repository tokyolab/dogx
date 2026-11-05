//go:build linux || darwin || freebsd

package process

import (
	"os/exec"
	"syscall"
	"testing"
)

func configure(_ *exec.Cmd) {}

func signalProcess(t testing.TB, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM to test process: %v", err)
	}
}

func runSignalSender(t testing.TB) {
	t.Helper()
	t.Skip("private-console subprocess helper is only needed on Windows")
}
