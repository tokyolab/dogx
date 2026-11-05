package jobexecutor

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/zrpc"
)

func TestExecutorServerTransportSettings(t *testing.T) {
	c := zrpc.RpcServerConf{}
	c.Middlewares.StatConf.IgnoreContentMethods = []string{"/custom/Private"}
	c.MethodTimeouts = []zrpc.MethodTimeoutConf{
		{FullMethod: protocol.Executor_DispatchTask_FullMethodName, Timeout: 5 * time.Second},
		{FullMethod: "/custom/Other", Timeout: 4 * time.Second},
		{FullMethod: protocol.Executor_CancelInstance_FullMethodName, Timeout: 9 * time.Second},
	}
	wantOriginal := slices.Clone(c.MethodTimeouts)
	got, err := executorServerConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Health || got.Timeout <= 0 {
		t.Fatal("health or method timeout interceptor is disabled")
	}
	want := []zrpc.MethodTimeoutConf{
		{FullMethod: "/custom/Other", Timeout: 4 * time.Second},
		{FullMethod: protocol.Executor_DispatchTask_FullMethodName, Timeout: 5 * time.Second},
		{FullMethod: protocol.Executor_CancelInstance_FullMethodName, Timeout: time.Second},
	}
	if !reflect.DeepEqual(got.MethodTimeouts, want) || !reflect.DeepEqual(c.MethodTimeouts, wantOriginal) {
		t.Fatal("executor deadlines were not isolated from the original configuration")
	}
	for _, method := range []string{"/custom/Private", protocol.Executor_DispatchTask_FullMethodName, protocol.Executor_CancelInstance_FullMethodName} {
		if !slices.Contains(got.Middlewares.StatConf.IgnoreContentMethods, method) {
			t.Fatalf("missing content suppression for %s", method)
		}
	}
	repeated, err := executorServerConfig(got)
	if err != nil || len(c.Middlewares.StatConf.IgnoreContentMethods) != 1 || !reflect.DeepEqual(repeated, got) {
		t.Fatal("server configuration is not isolated or idempotent")
	}
}

func TestExecutorServerDefaultsOnlyOmittedDispatchTimeout(t *testing.T) {
	for _, configured := range []time.Duration{0, time.Second, 7 * time.Second} {
		c := zrpc.RpcServerConf{}
		want := 3 * time.Second
		if configured > 0 {
			c.MethodTimeouts = []zrpc.MethodTimeoutConf{{FullMethod: protocol.Executor_DispatchTask_FullMethodName, Timeout: configured}}
			want = configured
		}
		got, err := executorServerConfig(c)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.MethodTimeouts) != 2 || got.MethodTimeouts[0].FullMethod != protocol.Executor_DispatchTask_FullMethodName ||
			got.MethodTimeouts[0].Timeout != want || got.MethodTimeouts[1].FullMethod != protocol.Executor_CancelInstance_FullMethodName ||
			got.MethodTimeouts[1].Timeout != time.Second {
			t.Fatalf("unexpected method budgets for configured=%v: %+v", configured, got.MethodTimeouts)
		}
	}
}

func TestExecutorServerRejectsInvalidMethodTimeoutsBeforeConstruction(t *testing.T) {
	for _, test := range []struct {
		name    string
		methods []zrpc.MethodTimeoutConf
		message string
	}{
		{"explicit zero dispatch", []zrpc.MethodTimeoutConf{{FullMethod: protocol.Executor_DispatchTask_FullMethodName}}, "must be positive"},
		{"negative dispatch", []zrpc.MethodTimeoutConf{{FullMethod: protocol.Executor_DispatchTask_FullMethodName, Timeout: -time.Second}}, "must be positive"},
		{"duplicate dispatch", []zrpc.MethodTimeoutConf{{FullMethod: protocol.Executor_DispatchTask_FullMethodName, Timeout: time.Second}, {FullMethod: protocol.Executor_DispatchTask_FullMethodName, Timeout: 2 * time.Second}}, "duplicate method timeout"},
		{"duplicate cancellation", []zrpc.MethodTimeoutConf{{FullMethod: protocol.Executor_CancelInstance_FullMethodName, Timeout: time.Second}, {FullMethod: protocol.Executor_CancelInstance_FullMethodName, Timeout: 2 * time.Second}}, "duplicate method timeout"},
		{"duplicate other method", []zrpc.MethodTimeoutConf{{FullMethod: "/custom/Other", Timeout: time.Second}, {FullMethod: "/custom/Other", Timeout: 2 * time.Second}}, "duplicate method timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			server, err := NewServer(zrpc.RpcServerConf{MethodTimeouts: test.methods}, func() *RPCAdapter {
				called = true
				return &RPCAdapter{}
			})
			if server != nil || err == nil || !strings.Contains(err.Error(), test.message) || called {
				t.Fatalf("invalid timeout reached construction or provider: server=%v err=%v providerCalled=%v", server, err, called)
			}
		})
	}
}

func TestExecutorServerRequiresAdapterAndSeparateAuth(t *testing.T) {
	if _, err := NewServer(zrpc.RpcServerConf{}, nil); err == nil {
		t.Fatal("nil adapter provider accepted")
	}
	called := false
	if _, err := NewServer(zrpc.RpcServerConf{Auth: true}, func() *RPCAdapter {
		called = true
		return &RPCAdapter{}
	}); err == nil {
		t.Fatal("native auth accepted on executor listener")
	}
	if called {
		t.Fatal("rejected configuration evaluated the adapter provider")
	}
}
