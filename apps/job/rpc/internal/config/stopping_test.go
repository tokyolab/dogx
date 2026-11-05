package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestStoppingDeploymentDefaultsAndValidation(t *testing.T) {
	content, err := os.ReadFile("../../etc/job-rpc.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	example := strings.ReplaceAll(string(content), "\r\n", "\n")
	start, end := strings.Index(example, "Stopping:\n"), strings.Index(example, "Postgres:\n")
	if start < 0 || end <= start {
		t.Fatal("missing stop configuration blocks")
	}
	want := validExecutionConfig()
	for _, input := range []string{example, example[:start] + example[end:]} {
		var c Config
		if err := conf.LoadFromYamlBytes([]byte(input), &c); err != nil {
			t.Fatal(err)
		}
		if c.Stopping != want.Stopping || c.CancelNotification != want.CancelNotification || c.PrepareExecution() != nil {
			t.Fatal("stopping example and omitted sections differ from approved defaults")
		}
	}
	for _, edit := range []func(*Config){
		func(c *Config) { c.Stopping.Capacity = 0 }, func(c *Config) { c.Stopping.Concurrency = 0 },
		func(c *Config) { c.Stopping.BatchSize = 0 }, func(c *Config) { c.Stopping.MaxBatches = 0 },
		func(c *Config) { c.Stopping.RunTimeout = 0 }, func(c *Config) { c.Stopping.SQLTimeout = 0 },
		func(c *Config) { c.Stopping.RecoveryPageSize = 0 }, func(c *Config) { c.Stopping.RecoveryTimeout = 0 },
		func(c *Config) { c.CancelNotification.InstanceCapacity = 0 },
		func(c *Config) { c.CancelNotification.PageSize = 0 }, func(c *Config) { c.CancelNotification.PageSize = 33 },
		func(c *Config) { c.CancelNotification.TargetCapacity = 0 }, func(c *Config) { c.CancelNotification.Concurrency = 0 },
		func(c *Config) { c.CancelNotification.QueryTimeout = 0 }, func(c *Config) { c.CancelNotification.InstanceTimeout = 0 },
	} {
		c := validExecutionConfig()
		edit(&c)
		if c.PrepareExecution() == nil {
			t.Fatal("invalid stop setting passed host validation")
		}
	}
	var c StoppingConf
	if err := conf.LoadFromYamlBytes([]byte("BatchSize: 300\nRecoveryPageSize: 300\nRunTimeout: 3s\n"), &c); err != nil {
		t.Fatal(err)
	}
	if c.Validate() != nil || c.BatchSize != 300 || c.RecoveryPageSize != 300 || c.RunTimeout != 3*time.Second {
		t.Fatal("database deployment limits were silently clamped")
	}
}
