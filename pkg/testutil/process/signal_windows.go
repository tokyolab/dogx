//go:build windows

package process

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	createNewConsole    = 0x00000010
	signalSenderEnv     = "DOGX_TEST_PRIVATE_CONSOLE_SIGNAL_SENDER"
	signalConfirmedLine = "DOGX_PROCESS_SIGNAL_CONFIRMED"
)

func configure(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: createNewConsole, HideWindow: true}
}

func signalProcess(t testing.TB, target *exec.Cmd) {
	t.Helper()
	if target.SysProcAttr == nil || target.SysProcAttr.CreationFlags&createNewConsole == 0 || !target.SysProcAttr.HideWindow {
		t.Fatal("a started test process with a hidden private console is required")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatalf("locate the current test binary: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestProcessSignalSender$")
	Configure(cmd)
	// Override only this child environment. Filter case-insensitively because
	// Windows environment names are case-insensitive; never mutate os.Args or
	// the parent environment to select the helper or communicate the target.
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !strings.EqualFold(key, signalSenderEnv) {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, signalSenderEnv+"=1")
	cmd.Stdin = strings.NewReader(strconv.Itoa(target.Process.Pid))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("send private-console termination signal: %v\n%s", err, output)
	}
	// A test binary exits successfully even if the fixed wrapper is missing or
	// skips its work. Require the sender's explicit receipt acknowledgement.
	for _, line := range strings.Split(string(output), "\n") {
		if strings.TrimSuffix(line, "\r") == signalConfirmedLine {
			return
		}
	}
	t.Fatal("private-console signal was not confirmed; TestProcessSignalSender must call RunSignalSender")
}

func runSignalSender(t testing.TB) {
	t.Helper()
	if os.Getenv(signalSenderEnv) != "1" {
		t.Skip("private-console subprocess helper")
	}
	input, err := io.ReadAll(io.LimitReader(os.Stdin, 32))
	if err != nil {
		t.Fatalf("read private-console target: %v", err)
	}
	pid, err := strconv.ParseUint(string(input), 10, 32)
	if err != nil || pid == 0 || pid == uint64(os.Getpid()) || pid == uint64(os.Getppid()) {
		t.Fatal("refuse an invalid or parent console target")
	}
	kernel := windows.NewLazySystemDLL("kernel32.dll")
	freeConsole := kernel.NewProc("FreeConsole")
	attachConsole := kernel.NewProc("AttachConsole")
	getMembers := kernel.NewProc("GetConsoleProcessList")
	setHandler := kernel.NewProc("SetConsoleCtrlHandler")
	generate := kernel.NewProc("GenerateConsoleCtrlEvent")
	if ok, _, err := freeConsole.Call(); ok == 0 {
		t.Fatalf("detach sender's private console: %v", err)
	}
	if ok, _, err := attachConsole.Call(uintptr(pid)); ok == 0 {
		t.Fatalf("attach target's private console: %v", err)
	}
	defer func() { _, _, _ = freeConsole.Call() }()
	// Refuse any console containing processes other than this sender and its
	// target. This check precedes the broadcast even when attachment succeeds.
	var members [3]uint32
	count, _, err := getMembers.Call(uintptr(unsafe.Pointer(&members[0])), uintptr(len(members)))
	if count != 2 || !((members[0] == uint32(pid) && members[1] == uint32(os.Getpid())) ||
		(members[1] == uint32(pid) && members[0] == uint32(os.Getpid()))) {
		t.Fatalf("refuse a console that is not exclusive to target and sender: members=%d error=%v", count, err)
	}
	// AttachConsole resets the native handler table, including the handler Go
	// installed during runtime initialization. Reinstalling a local bridge is
	// necessary: signal.Notify alone does not reinstall that runtime callback.
	// Receive the actual OS event without allowing CTRL_BREAK to kill the sender.
	interrupts := make(chan os.Signal, 1)
	handler := syscall.NewCallback(func(event uint32) uintptr {
		if event != syscall.CTRL_BREAK_EVENT {
			return 0
		}
		select {
		case interrupts <- os.Interrupt:
		default:
		}
		return 1
	})
	if ok, _, err := setHandler.Call(handler, 1); ok == 0 {
		t.Fatalf("install sender's interrupt handler: %v", err)
	}
	defer func() { _, _, _ = setHandler.Call(handler, 0) }()
	if ok, _, err := generate.Call(syscall.CTRL_BREAK_EVENT, 0); ok == 0 {
		t.Fatalf("GenerateConsoleCtrlEvent: %v", err)
	}
	select {
	case <-interrupts:
	case <-time.After(5 * time.Second):
		t.Fatal("the target's private console did not deliver CTRL_BREAK")
	}
	if _, err := fmt.Fprintln(os.Stdout, signalConfirmedLine); err != nil {
		t.Fatalf("confirm private-console signal receipt: %v", err)
	}
}
