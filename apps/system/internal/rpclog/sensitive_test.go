package rpclog

import (
	"reflect"
	"testing"

	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"github.com/zeromicro/go-zero/zrpc"
)

func TestProtectClientContentRegistersEverySensitiveMethod(t *testing.T) {
	var registered []string
	protectClientContent(func(method string) {
		registered = append(registered, method)
	})

	if !reflect.DeepEqual(registered, sensitiveSystemMethods) {
		t.Fatalf("registered methods = %v, want %v", registered, sensitiveSystemMethods)
	}
}

func TestProtectServerContentPreservesExistingMethodsAndAvoidsDuplicates(t *testing.T) {
	conf := zrpc.RpcServerConf{}
	conf.Middlewares.StatConf.IgnoreContentMethods = []string{
		"/example.Service/Existing",
		system.System_Login_FullMethodName,
	}

	ProtectServerContent(&conf)
	ProtectServerContent(&conf)

	want := append([]string{"/example.Service/Existing"}, sensitiveSystemMethods...)
	if !reflect.DeepEqual(conf.Middlewares.StatConf.IgnoreContentMethods, want) {
		t.Fatalf(
			"ignored methods = %v, want %v",
			conf.Middlewares.StatConf.IgnoreContentMethods,
			want,
		)
	}
}

func TestSensitiveSystemMethodsCoverCredentialBearingRPCs(t *testing.T) {
	want := map[string]struct{}{
		system.System_Login_FullMethodName:              {},
		system.System_RefreshCredentials_FullMethodName: {},
		system.System_RevokeSession_FullMethodName:      {},
		system.System_ChangePassword_FullMethodName:     {},
	}
	for _, method := range sensitiveSystemMethods {
		delete(want, method)
	}
	if len(want) != 0 {
		t.Fatalf("sensitive RPC methods are missing: %v", want)
	}
}
