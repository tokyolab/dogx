package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
)

func TestResultPersistenceDeploymentDefaults(t *testing.T) {
	var value ResultPersistenceConf
	if err := conf.LoadFromYamlBytes([]byte("{}"), &value); err != nil {
		t.Fatal(err)
	}
	if value != (ResultPersistenceConf{Capacity: 512, Concurrency: 8, SaveTimeout: 2 * time.Second}) {
		t.Fatalf("unexpected result persistence defaults: %+v", value)
	}
	value.Capacity = 1
	if err := value.Validate(); err != nil {
		t.Fatalf("concurrency may exceed capacity: %v", err)
	}
}

func TestResultPersistenceRejectsExplicitNonpositiveValues(t *testing.T) {
	for _, field := range []string{"Capacity", "Concurrency", "SaveTimeout"} {
		for _, number := range []int{0, -1} {
			t.Run(fmt.Sprintf("%s/%d", field, number), func(t *testing.T) {
				input := fmt.Sprintf("%s: %d\n", field, number)
				if field == "SaveTimeout" {
					input = fmt.Sprintf("%s: %ds\n", field, number)
				}
				var value ResultPersistenceConf
				if err := conf.LoadFromYamlBytes([]byte(input), &value); err == nil {
					t.Fatal("explicit nonpositive value was accepted")
				}
			})
		}
	}
}

func TestExecutionExampleAndOmittedResultDefaults(t *testing.T) {
	content, err := os.ReadFile("../../etc/job-rpc.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	example := strings.ReplaceAll(string(content), "\r\n", "\n")
	withoutResult := strings.Replace(example, "ResultPersistence:\n  Capacity: 512\n  Concurrency: 8\n  SaveTimeout: 2s\n", "", 1)
	if withoutResult == example {
		t.Fatal("example result persistence block was not removed")
	}
	for _, input := range []string{
		example,
		withoutResult,
	} {
		var value Config
		if err := conf.LoadFromYamlBytes([]byte(input), &value); err != nil {
			t.Fatal(err)
		}
		if err := value.PrepareExecution(); err != nil {
			t.Fatal(err)
		}
		if value.ResultPersistence != (ResultPersistenceConf{Capacity: 512, Concurrency: 8, SaveTimeout: 2 * time.Second}) {
			t.Fatalf("unexpected result persistence defaults: %+v", value.ResultPersistence)
		}
		if value.ExecutionRPC.ListenOn != "0.0.0.0:9003" || value.ExecutionRPC.Etcd.Key != "/dogx/development/default/rpc/job-execution" || !value.ExecutionRPC.Health {
			t.Fatal("example execution transport changed")
		}
		if !reflect.DeepEqual(value.ExecutionRPC.ServiceConf, value.ServiceConf) {
			t.Fatal("execution process settings differ from the host")
		}
	}
}

func TestPrepareExecutionCopiesOnlyHostServiceConfiguration(t *testing.T) {
	value := validExecutionConfig()
	value.Auth = true
	value.StrictControl = true
	value.ServiceConf.Name = "host-process"
	value.ServiceConf.Mode = service.DevMode
	value.ServiceConf.Log.Level = "error"
	value.ServiceConf.MetricsUrl = "http://metrics.invalid"
	value.ServiceConf.Telemetry.Name = "host-traces"
	value.ExecutionRPC.ServiceConf.Name = "decoder-only"
	value.ExecutionRPC.ServiceConf.Mode = service.ProMode
	value.ExecutionRPC.Timeout = 137
	value.ExecutionRPC.Health = false
	value.ExecutionRPC.CpuThreshold = 873
	value.ExecutionRPC.MethodTimeouts = []zrpc.MethodTimeoutConf{{FullMethod: "/example/Call", Timeout: time.Second}}
	management := value.RpcServerConf
	wantExecution := value.ExecutionRPC
	wantExecution.ServiceConf = management.ServiceConf
	if err := value.PrepareExecution(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(value.RpcServerConf, management) {
		t.Fatal("management RPC configuration was changed")
	}
	if !reflect.DeepEqual(value.ExecutionRPC, wantExecution) {
		t.Fatal("execution transport settings were changed while copying host settings")
	}
}

func TestPrepareExecutionRejectsInvalidSettingsWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{"native auth", func(c *Config) { c.ExecutionRPC.Auth = true }},
		{"missing listener", func(c *Config) { c.ExecutionRPC.ListenOn = "" }},
		{"malformed listener", func(c *Config) { c.ExecutionRPC.ListenOn = "credential-sentinel" }},
		{"dynamic port", func(c *Config) { c.ExecutionRPC.ListenOn = "127.0.0.1:0" }},
		{"invalid port", func(c *Config) { c.ExecutionRPC.ListenOn = "127.0.0.1:65536" }},
		{"same listener", func(c *Config) { c.ExecutionRPC.ListenOn = c.ListenOn }},
		{"wildcard listener", func(c *Config) { c.ExecutionRPC.ListenOn = "127.0.0.1:9002" }},
		{"missing etcd hosts", func(c *Config) { c.ExecutionRPC.Etcd.Hosts = nil }},
		{"empty etcd host", func(c *Config) { c.ExecutionRPC.Etcd.Hosts = []string{" "} }},
		{"missing etcd key", func(c *Config) { c.ExecutionRPC.Etcd.Key = "" }},
		{"blank etcd key", func(c *Config) { c.ExecutionRPC.Etcd.Key = " " }},
		{"shared discovery key", func(c *Config) { c.ExecutionRPC.Etcd.Key = c.Etcd.Key }},
		{"zero result capacity", func(c *Config) { c.ResultPersistence.Capacity = 0 }},
		{"negative result concurrency", func(c *Config) { c.ResultPersistence.Concurrency = -1 }},
		{"zero result timeout", func(c *Config) { c.ResultPersistence.SaveTimeout = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validExecutionConfig()
			test.edit(&value)
			before := value
			err := value.PrepareExecution()
			if err == nil {
				t.Fatal("invalid configuration was accepted")
			}
			if strings.Contains(err.Error(), "credential-sentinel") {
				t.Fatal("configuration error exposed a value")
			}
			if !reflect.DeepEqual(value, before) {
				t.Fatal("failed preparation changed configuration")
			}
		})
	}
	var value *Config
	if err := value.PrepareExecution(); err == nil {
		t.Fatal("nil configuration was accepted")
	}
}

func TestPrepareExecutionDiscoveryCollisionScope(t *testing.T) {
	tests := []struct {
		name      string
		hosts     []string
		collision bool
	}{
		{"same endpoint", []string{"127.0.0.1:2379"}, true},
		{"native endpoint scheme", []string{"http://127.0.0.1:2379/"}, true},
		{"one shared endpoint", []string{"127.0.0.2:2379", "127.0.0.1:2379"}, true},
		{"different cluster", []string{"127.0.0.2:2379"}, false},
		{"unresolved alias", []string{"etcd.example.invalid:2379"}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validExecutionConfig()
			value.ExecutionRPC.Etcd.Key = value.Etcd.Key
			value.ExecutionRPC.Etcd.Hosts = test.hosts
			if err := value.PrepareExecution(); (err != nil) != test.collision {
				t.Fatalf("collision rejected=%v, want %v", err != nil, test.collision)
			}
		})
	}
}

func TestPrepareExecutionRejectsInheritedManagementKey(t *testing.T) {
	content, err := os.ReadFile("../../etc/job-rpc.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	input := strings.Replace(string(content), "    Key: /dogx/development/default/rpc/job-execution", "", 1)
	var value Config
	if err := conf.LoadFromYamlBytes([]byte(input), &value); err != nil {
		t.Fatal(err)
	}
	if value.ExecutionRPC.Etcd.Key != value.Etcd.Key {
		t.Fatal("native etcd inheritance changed")
	}
	if err := value.PrepareExecution(); err == nil {
		t.Fatal("inherited management discovery key was accepted")
	}
}

func TestListenerOverlapLiteralAddresses(t *testing.T) {
	tests := []struct {
		left, right string
		want        bool
	}{
		{"127.0.0.1:9002", "127.0.0.1:9002", true},
		{"0.0.0.0:9002", "127.0.0.1:9002", true},
		{"127.0.0.1:9002", "0.0.0.0:9002", true},
		{":9002", "rpc.example.invalid:9002", true},
		{"[::]:9002", "127.0.0.1:9002", true},
		{"[::1]:9002", "[0:0:0:0:0:0:0:1]:9002", true},
		{"127.0.0.1:9002", "[::ffff:127.0.0.1]:09002", true},
		{"RPC.EXAMPLE.INVALID:9002", "rpc.example.invalid:9002", true},
		{"0.0.0.0:9002", "127.0.0.1:9003", false},
		{"127.0.0.1:9002", "127.0.0.2:9002", false},
		{"0.0.0.0:9002", "[::1]:9002", false},
		{"localhost:9002", "127.0.0.1:9002", false},
	}
	for _, test := range tests {
		t.Run(test.left+"/"+test.right, func(t *testing.T) {
			if got := rpchost.ListenersOverlap(test.left, test.right); got != test.want {
				t.Fatalf("overlap=%v, want %v", got, test.want)
			}
		})
	}
}

func validExecutionConfig() Config {
	return Config{
		RpcServerConf: zrpc.RpcServerConf{
			ServiceConf: service.ServiceConf{Name: "job-rpc"},
			ListenOn:    "0.0.0.0:9002",
			Etcd:        discov.EtcdConf{Hosts: []string{"127.0.0.1:2379"}, Key: "/test/job"},
		},
		ExecutionRPC: zrpc.RpcServerConf{
			ServiceConf: service.ServiceConf{Name: "job-rpc"},
			ListenOn:    "0.0.0.0:9003",
			Etcd:        discov.EtcdConf{Hosts: []string{"127.0.0.1:2379"}, Key: "/test/job-execution"},
		},
		ResultPersistence:  ResultPersistenceConf{Capacity: 512, Concurrency: 8, SaveTimeout: 2 * time.Second},
		InstanceCheck:      InstanceCheckConf{Capacity: 512, Concurrency: 8, Timeout: 2 * time.Second},
		InstanceRecovery:   InstanceRecoveryConf{Interval: 2 * time.Minute, PageSize: 256, RunTimeout: 5 * time.Second, SQLTimeout: time.Second},
		UnboundRecovery:    UnboundRecoveryConf{ScanInterval: 10 * time.Second, PageSize: 128, RunTimeout: 5 * time.Second, OperationTimeout: time.Second, ObservationPeriod: 30 * time.Second},
		Stopping:           StoppingConf{Capacity: 256, Concurrency: 2, BatchSize: 256, MaxBatches: 4, RunTimeout: 2 * time.Second, SQLTimeout: time.Second, RecoveryPageSize: 256, RecoveryTimeout: 5 * time.Second},
		CancelNotification: CancelNotificationConf{InstanceCapacity: 256, PageSize: 32, TargetCapacity: 256, Concurrency: 16, QueryTimeout: time.Second, InstanceTimeout: time.Minute},
		Dispatch:           DispatchConf{Concurrency: 64, WaitingCapacity: 256, ScanInterval: time.Second, TotalTimeout: 10 * time.Second, RPCTimeout: 3 * time.Second, BusyDelay: 5 * time.Second},
	}
}

func TestSchedulerDeploymentDefaults(t *testing.T) {
	var value SchedulerConf
	if err := conf.LoadFromYamlBytes([]byte("Prefix: /dogx/test/default\n"), &value); err != nil {
		t.Fatal(err)
	}
	if value.QueueCapacity != 1024 || value.CreationConcurrency != 32 || value.CreationBudget != 3*time.Second || value.LeaseTTLSeconds != 15 || value.RecoveryLimit != 1024 || value.QueryTimeout != 5*time.Second {
		t.Fatalf("defaults=%+v", value)
	}
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSchedulerConfigRejectsNonpositiveCapacityAndBudget(t *testing.T) {
	valid := SchedulerConf{QueueCapacity: 1024, CreationConcurrency: 32, CreationBudget: 3 * time.Second, LeaseTTLSeconds: 15, RecoveryLimit: 1024, QueryTimeout: 5 * time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"queue", "concurrency", "creation budget", "lease", "recovery", "query budget"} {
		t.Run(field, func(t *testing.T) {
			value := valid
			switch field {
			case "queue":
				value.QueueCapacity = 0
			case "concurrency":
				value.CreationConcurrency = -1
			case "creation budget":
				value.CreationBudget = 0
			case "lease":
				value.LeaseTTLSeconds = 0
			case "recovery":
				value.RecoveryLimit = 0
			case "query budget":
				value.QueryTimeout = 0
			}
			if err := value.Validate(); err == nil {
				t.Fatal("invalid scheduler configuration accepted")
			}
		})
	}
}
