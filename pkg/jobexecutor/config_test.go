package jobexecutor

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
)

const executorConfigYAML = `
GroupName: test-workers
Token: '11111111111141118111111111111111'
Server:
  Name: decoder-only
  ListenOn: 0.0.0.0:9004
  Etcd:
    Hosts:
      - 127.0.0.1:2379
    Key: /test/executor
  Auth: false
  Health: true
ReportClient:
  Etcd:
    Hosts:
      - 127.0.0.1:2379
    Key: /test/job-execution
`

func TestExecutorConfigDecodesRuntimeDefaults(t *testing.T) {
	var config Config
	if err := conf.LoadFromYamlBytes([]byte(executorConfigYAML), &config); err != nil {
		t.Fatal(err)
	}
	want := RuntimeConfig{Concurrency: 32, QueueCapacity: 128, ReportCapacity: 256, ReportConcurrency: 16}
	if config.Runtime != want {
		t.Fatalf("runtime defaults = %+v", config.Runtime)
	}
	if !config.ReportClient.NonBlock || config.ReportClient.Timeout != 2000 || !config.Server.Health {
		t.Fatal("native RPC defaults were lost")
	}
	config.Runtime = RuntimeConfig{Concurrency: 4, QueueCapacity: 1, ReportCapacity: 1, ReportConcurrency: 8}
	if err := config.Validate(); err != nil {
		t.Fatalf("independent limits must not impose relative size constraints: %v", err)
	}
}

func TestExecutorConfigRejectsExplicitNonpositiveRuntimeValues(t *testing.T) {
	for _, field := range []string{"Concurrency", "QueueCapacity", "ReportCapacity", "ReportConcurrency"} {
		for _, number := range []int{0, -1} {
			t.Run(fmt.Sprintf("%s/%d", field, number), func(t *testing.T) {
				input := executorConfigYAML + fmt.Sprintf("Runtime:\n  %s: %d\n", field, number)
				var config Config
				if err := conf.LoadFromYamlBytes([]byte(input), &config); err == nil {
					t.Fatal("explicit nonpositive runtime setting accepted")
				}
			})
		}
	}
}

func TestExecutorConfigRejectsInvalidCommunicationWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Config)
	}{
		{"group", func(c *Config) { c.GroupName = "" }},
		{"token", func(c *Config) { c.Token = "secret-sentinel" }},
		{"server auth", func(c *Config) { c.Server.Auth = true }},
		{"server address", func(c *Config) { c.Server.ListenOn = "secret-sentinel" }},
		{"dynamic port", func(c *Config) { c.Server.ListenOn = "127.0.0.1:0" }},
		{"server hosts", func(c *Config) { c.Server.Etcd.Hosts = nil }},
		{"server key", func(c *Config) { c.Server.Etcd.Key = "" }},
		{"client app", func(c *Config) { c.ReportClient.App = "secret-sentinel" }},
		{"client token", func(c *Config) { c.ReportClient.Token = "secret-sentinel" }},
		{"client hosts", func(c *Config) { c.ReportClient.Etcd.Hosts = nil }},
		{"client key", func(c *Config) { c.ReportClient.Etcd.Key = "" }},
		{"client endpoints", func(c *Config) { c.ReportClient.Endpoints = []string{"secret-sentinel"} }},
		{"client target", func(c *Config) { c.ReportClient.Target = "secret-sentinel" }},
		{"shared discovery target", func(c *Config) { c.ReportClient.Etcd.Key = c.Server.Etcd.Key }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := validExecutorConfig(t)
			test.edit(&config)
			before := config
			err := config.Validate()
			if err == nil {
				t.Fatal("invalid executor configuration accepted")
			}
			if strings.Contains(err.Error(), "secret-sentinel") || strings.Contains(err.Error(), adapterTestToken) {
				t.Fatal("configuration validation exposed a value")
			}
			if !reflect.DeepEqual(config, before) {
				t.Fatal("validation mutated configuration")
			}
		})
	}
}

func TestExecutorConfigPrepareCopiesOnlyHostProcessSettings(t *testing.T) {
	config := validExecutorConfig(t)
	config.Server.Timeout = 137
	config.Server.Health = false
	config.Server.CpuThreshold = 833
	host := zrpc.RpcServerConf{
		ServiceConf: service.ServiceConf{Name: "system-rpc", Mode: service.DevMode, MetricsUrl: "http://metrics.example.invalid"},
		ListenOn:    "0.0.0.0:9001", Auth: true, StrictControl: true, Timeout: 5000,
		Etcd: discov.EtcdConf{Hosts: []string{"127.0.0.1:2379"}, Key: "/test/system"},
	}
	host.Log.Level = "error"
	host.Telemetry.Name = "host-traces"
	beforeHost := host
	want := config
	want.Server.ServiceConf = host.ServiceConf
	if err := config.Prepare(host); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(config, want) {
		t.Fatal("preparation changed transport, credentials or runtime settings")
	}
	if !reflect.DeepEqual(host, beforeHost) {
		t.Fatal("preparation changed the host's RPC configuration")
	}
}

func TestExecutorConfigPrepareRejectsKnownHostConflicts(t *testing.T) {
	for _, test := range []struct {
		name string
		host zrpc.RpcServerConf
	}{
		{"same listener", zrpc.RpcServerConf{ListenOn: "0.0.0.0:9004"}},
		{"wildcard listener", zrpc.RpcServerConf{ListenOn: "127.0.0.1:9004"}},
		{"same server discovery", zrpc.RpcServerConf{ListenOn: "0.0.0.0:9001", Etcd: discov.EtcdConf{Hosts: []string{"http://127.0.0.1:2379"}, Key: "/test/executor"}}},
		{"report targets host", zrpc.RpcServerConf{ListenOn: "0.0.0.0:9001", Etcd: discov.EtcdConf{Hosts: []string{"127.0.0.1:2379"}, Key: "/test/job-execution"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := validExecutorConfig(t)
			before := config
			if err := config.Prepare(test.host); err == nil {
				t.Fatal("host endpoint conflict accepted")
			}
			if !reflect.DeepEqual(config, before) {
				t.Fatal("failed preparation changed configuration")
			}
		})
	}
	var missing *Config
	if err := missing.Prepare(zrpc.RpcServerConf{}); err == nil {
		t.Fatal("nil executor configuration accepted")
	}
}

func TestExecutorConfigAllowsSameKeyOnDistinctKnownClusters(t *testing.T) {
	config := validExecutorConfig(t)
	config.ReportClient.Etcd.Key = config.Server.Etcd.Key
	config.ReportClient.Etcd.Hosts = []string{"127.0.0.2:2379"}
	host := zrpc.RpcServerConf{
		ListenOn: "0.0.0.0:9001",
		Etcd:     discov.EtcdConf{Hosts: []string{"127.0.0.3:2379"}, Key: config.Server.Etcd.Key},
	}
	if err := config.Prepare(host); err != nil {
		t.Fatal(err)
	}
}

func validExecutorConfig(t *testing.T) Config {
	t.Helper()
	var config Config
	if err := conf.LoadFromYamlBytes([]byte(executorConfigYAML), &config); err != nil {
		t.Fatal(err)
	}
	return config
}
