package rpchost

import (
	"strings"
	"testing"

	"github.com/zeromicro/go-zero/core/discov"
)

func TestValidateListenerRequiresFixedPortWithoutResolvingHost(t *testing.T) {
	for _, address := range []string{":9004", "127.0.0.1:9004", "[::1]:9004", "rpc.example.invalid:9004"} {
		if err := ValidateListener(address); err != nil {
			t.Fatalf("valid listener rejected: %v", err)
		}
	}
	for _, address := range []string{"", "secret-sentinel", "127.0.0.1", ":0", ":-1", ":65536", ":http"} {
		if err := ValidateListener(address); err == nil {
			t.Fatal("invalid listener accepted")
		} else if strings.Contains(err.Error(), "secret-sentinel") {
			t.Fatal("listener validation exposed its supplied value")
		}
	}
}

func TestValidateDiscoveryRequiresNativeNonblankSettings(t *testing.T) {
	valid := discov.EtcdConf{Hosts: []string{"etcd.example.invalid:2379"}, Key: "/rpc/test"}
	if err := ValidateDiscovery(valid); err != nil {
		t.Fatal(err)
	}
	for _, config := range []discov.EtcdConf{
		{Key: "/rpc/test"},
		{Hosts: []string{"etcd.example.invalid:2379"}},
		{Hosts: []string{""}, Key: "/rpc/test"},
		{Hosts: []string{" "}, Key: "/rpc/test"},
		{Hosts: []string{"etcd.example.invalid:2379"}, Key: " "},
	} {
		if err := ValidateDiscovery(config); err == nil {
			t.Fatal("incomplete discovery configuration accepted")
		}
	}
}

func TestListenerOverlapUsesOnlyKnownLiteralAddresses(t *testing.T) {
	for _, test := range []struct {
		left, right string
		want        bool
	}{
		{"0.0.0.0:9004", "127.0.0.1:9004", true},
		{"[::]:9004", "127.0.0.1:9004", true},
		{"RPC.EXAMPLE.INVALID:9004", "rpc.example.invalid:9004", true},
		{"127.0.0.1:9004", "[::ffff:127.0.0.1]:09004", true},
		{"127.0.0.1:9004", "127.0.0.1:9001", false},
		{"127.0.0.1:9004", "localhost:9004", false},
		{"invalid", "invalid", false},
	} {
		if got := ListenersOverlap(test.left, test.right); got != test.want {
			t.Fatalf("ListenersOverlap(%q, %q) = %v", test.left, test.right, got)
		}
	}
}

func TestDiscoveryOverlapRequiresSameKeyAndKnownSharedEndpoint(t *testing.T) {
	left := discov.EtcdConf{Hosts: []string{"127.0.0.1:2379"}, Key: "/rpc/test"}
	for _, test := range []struct {
		name  string
		right discov.EtcdConf
		want  bool
	}{
		{"same", left, true},
		{"scheme", discov.EtcdConf{Hosts: []string{"http://127.0.0.1:2379/"}, Key: left.Key}, true},
		{"mapped IP", discov.EtcdConf{Hosts: []string{"https://[::ffff:127.0.0.1]:02379"}, Key: left.Key}, true},
		{"shared member", discov.EtcdConf{Hosts: []string{"127.0.0.2:2379", left.Hosts[0]}, Key: left.Key}, true},
		{"different key", discov.EtcdConf{Hosts: left.Hosts, Key: "/rpc/other"}, false},
		{"different cluster", discov.EtcdConf{Hosts: []string{"127.0.0.2:2379"}, Key: left.Key}, false},
		{"unresolved alias", discov.EtcdConf{Hosts: []string{"localhost:2379"}, Key: left.Key}, false},
		{"direct host", discov.EtcdConf{}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := DiscoveryOverlap(left, test.right); got != test.want {
				t.Fatalf("DiscoveryOverlap = %v, want %v", got, test.want)
			}
		})
	}
}
