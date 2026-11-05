//go:build windows

package rpchost

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func configureHostSignalProcess(cmd *exec.Cmd) {
	// A hidden private console isolates CTRL_BREAK from the test runner and
	// the user's console. The sender below inherits only this private console.
	const createNewConsole = 0x00000010
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: createNewConsole, HideWindow: true}
}

func testHostSignalBeforeRun(t *testing.T) {
	t.Skip("Windows installs application signal handling when Run begins")
}

func sendHostTestSignal() error {
	cmd := exec.Command(os.Args[0], "-test.run=^TestHostControlSignalSender$")
	cmd.Env = append(os.Environ(), "DOGX_RPC_HOST_SIGNAL_SENDER=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("send host console signal: %w: %s", err, output)
	}
	return nil
}

func TestHostControlSignalSender(t *testing.T) {
	if os.Getenv("DOGX_RPC_HOST_SIGNAL_SENDER") != "1" {
		t.Skip("private-console subprocess helper")
	}
	// Both this sender and the host belong to the private console. Receive our
	// own event as well: Windows does not implement signal.Ignore for CTRL_BREAK.
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	defer signal.Stop(interrupts)
	generate := syscall.NewLazyDLL("kernel32.dll").NewProc("GenerateConsoleCtrlEvent")
	ok, _, err := generate.Call(syscall.CTRL_BREAK_EVENT, 0)
	if ok == 0 {
		t.Fatalf("GenerateConsoleCtrlEvent: %v", err)
	}
	select {
	case <-interrupts:
	case <-time.After(5 * time.Second):
		t.Fatal("the private console did not deliver CTRL_BREAK")
	}
}
