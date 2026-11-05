package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/tokyolab/dogx/pkg/rpchost"
	"github.com/zeromicro/go-zero/core/conf"
)

func TestPrepareExecutorUsesNormalizedHostProcessSettings(t *testing.T) {
	setExecutorExampleEnvironment(t)
	var loaded Config
	shutdown, err := rpchost.LoadConfig(filepath.Join("..", "..", "etc", "system-rpc.example.yaml"), &loaded)
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown.Normalize(&loaded.ServiceConf); err != nil {
		t.Fatal(err)
	}
	loaded.Auth = true
	loaded.StrictControl = true
	loaded.Redis.RedisConf = loaded.RedisConf
	loaded.Redis.Key = "test:native-rpc-auth"
	if err := loaded.RpcServerConf.Validate(); err != nil {
		t.Fatalf("host RPC fixture is invalid: %v", err)
	}
	before := loaded.RpcServerConf
	if err := loaded.PrepareExecutor(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.RpcServerConf, before) {
		t.Fatal("executor preparation changed the business RPC configuration")
	}
	if !reflect.DeepEqual(loaded.JobExecutor.Server.ServiceConf, before.ServiceConf) {
		t.Fatal("executor did not receive the normalized host process settings")
	}
	if loaded.JobExecutor.GroupName != "system.executor" || loaded.JobExecutor.Server.ListenOn != "0.0.0.0:9004" ||
		loaded.JobExecutor.Server.Etcd.Key != "/dogx/development/default/executor/system.executor" ||
		loaded.JobExecutor.ReportClient.Etcd.Key != "/dogx/development/default/rpc/job-execution" {
		t.Fatal("example executor routing differs from its declared deployment")
	}
	if loaded.JobExecutor.Server.Auth || !loaded.JobExecutor.Server.Health {
		t.Fatal("example executor transport did not keep separate authentication and health settings")
	}
}

func TestSystemExecutorOmittedRuntimeDefaultsAndExplicitZero(t *testing.T) {
	input := executorExampleInput(t)
	block := "  Runtime:\n    Concurrency: 32\n    QueueCapacity: 128\n    ReportCapacity: 256\n    ReportConcurrency: 16\n"
	withoutRuntime := strings.Replace(input, block, "", 1)
	if input == withoutRuntime {
		t.Fatal("example runtime block was not removed")
	}
	var loaded Config
	if err := conf.LoadFromYamlBytes([]byte(withoutRuntime), &loaded); err != nil {
		t.Fatal(err)
	}
	if err := loaded.PrepareExecutor(); err != nil {
		t.Fatal(err)
	}
	want := jobexecutor.RuntimeConfig{Concurrency: 32, QueueCapacity: 128, ReportCapacity: 256, ReportConcurrency: 16}
	if loaded.JobExecutor.Runtime != want {
		t.Fatalf("omitted runtime defaults = %+v", loaded.JobExecutor.Runtime)
	}
	zero := strings.Replace(input, "    Concurrency: 32", "    Concurrency: 0", 1)
	var invalid Config
	if err := conf.LoadFromYamlBytes([]byte(zero), &invalid); err != nil {
		t.Fatal(err)
	}
	if err := invalid.PrepareExecutor(); err == nil {
		t.Fatal("explicit zero configured concurrency was accepted")
	}
}

func TestSystemExecutorRejectsInheritedBusinessDiscoveryKey(t *testing.T) {
	input := executorExampleInput(t)
	input = strings.Replace(input, "Timeout: 5000", "Timeout: 5000\nEtcd:\n  Hosts:\n    - 127.0.0.1:2379\n  Key: /test/system-business", 1)
	input = strings.Replace(input, "      Key: /dogx/development/default/executor/system.executor", "", 1)
	var loaded Config
	if err := conf.LoadFromYamlBytes([]byte(input), &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.JobExecutor.Server.Etcd.Key != loaded.Etcd.Key {
		t.Fatal("native nested etcd inheritance changed")
	}
	before := loaded
	if err := loaded.PrepareExecutor(); err == nil {
		t.Fatal("executor inherited the business RPC discovery key without rejection")
	}
	if !reflect.DeepEqual(loaded, before) {
		t.Fatal("failed preparation changed system configuration")
	}
	var missing *Config
	if err := missing.PrepareExecutor(); err == nil {
		t.Fatal("nil system configuration accepted")
	}
}

func executorExampleInput(t *testing.T) string {
	t.Helper()
	setExecutorExampleEnvironment(t)
	content, err := os.ReadFile(filepath.Join("..", "..", "etc", "system-rpc.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(os.ExpandEnv(string(content)), "\r\n", "\n")
}

func setExecutorExampleEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("DOGX_ACCESS_SECRET", "0123456789abcdef0123456789abcdef")
	t.Setenv("DOGX_POSTGRES_PASSWORD", "postgres-test-password")
	t.Setenv("DOGX_REDIS_PASSWORD", "redis-test-password")
	t.Setenv("DOGX_JOB_EXECUTOR_TOKEN", "11111111111141118111111111111111")
}
