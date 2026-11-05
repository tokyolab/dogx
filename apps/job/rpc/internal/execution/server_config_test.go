package execution

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/jobexecutor/protocol"
	"github.com/zeromicro/go-zero/zrpc"
)

func TestResultServerTransportSettings(t *testing.T) {
	c := zrpc.RpcServerConf{}
	c.Middlewares.StatConf.IgnoreContentMethods = []string{"/custom/Private"}
	c.MethodTimeouts = []zrpc.MethodTimeoutConf{
		{FullMethod: protocol.ResultReceiver_ReportTaskResult_FullMethodName, Timeout: time.Second},
		{FullMethod: "/custom/Other", Timeout: 4 * time.Second},
	}
	wantOriginal := slices.Clone(c.MethodTimeouts)
	got := resultServerConfig(c)
	if !got.Health || got.Timeout <= 0 {
		t.Fatal("health or method timeout interceptor is disabled")
	}
	want := []zrpc.MethodTimeoutConf{
		{FullMethod: "/custom/Other", Timeout: 4 * time.Second},
		{FullMethod: protocol.ResultReceiver_ReportTaskResult_FullMethodName, Timeout: 2 * time.Second},
	}
	if !reflect.DeepEqual(got.MethodTimeouts, want) || !reflect.DeepEqual(c.MethodTimeouts, wantOriginal) {
		t.Fatal("result deadline was not isolated from the original configuration")
	}
	if !slices.Contains(got.Middlewares.StatConf.IgnoreContentMethods, protocol.ResultReceiver_ReportTaskResult_FullMethodName) ||
		!slices.Contains(got.Middlewares.StatConf.IgnoreContentMethods, "/custom/Private") {
		t.Fatal("result content suppression is incomplete")
	}
	if len(c.Middlewares.StatConf.IgnoreContentMethods) != 1 || !reflect.DeepEqual(resultServerConfig(got), got) {
		t.Fatal("server configuration is not isolated or idempotent")
	}
}

func TestResultServerRequiresReceiverAndSeparateAuth(t *testing.T) {
	if _, err := NewServer(zrpc.RpcServerConf{}, nil); err == nil {
		t.Fatal("nil receiver provider accepted")
	}
	called := false
	if _, err := NewServer(zrpc.RpcServerConf{Auth: true}, func() *Receiver {
		called = true
		return &Receiver{}
	}); err == nil {
		t.Fatal("native auth accepted on result listener")
	}
	if called {
		t.Fatal("rejected configuration evaluated the receiver provider")
	}
}
