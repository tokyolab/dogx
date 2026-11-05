package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestUnboundRecoveryDeploymentDefaultsAndOverrides(t *testing.T) {
	content, err := os.ReadFile("../../etc/job-rpc.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	example := strings.ReplaceAll(string(content), "\r\n", "\n")
	start, end := strings.Index(example, "UnboundRecovery:\n"), strings.Index(example, "Postgres:\n")
	if start < 0 || end <= start {
		t.Fatal("missing unbound recovery example block")
	}
	want := UnboundRecoveryConf{ScanInterval: 10 * time.Second, PageSize: 128, RunTimeout: 5 * time.Second, OperationTimeout: time.Second, ObservationPeriod: 30 * time.Second}
	for _, input := range []string{example, example[:start] + example[end:]} {
		var c Config
		if err := conf.LoadFromYamlBytes([]byte(input), &c); err != nil {
			t.Fatal(err)
		}
		if c.UnboundRecovery != want || c.PrepareExecution() != nil {
			t.Fatal("example and omitted recovery settings must load the approved defaults")
		}
	}
	var overridden UnboundRecoveryConf
	if err := conf.LoadFromYamlBytes([]byte("ScanInterval: 20s\nPageSize: 257\nRunTimeout: 8s\nOperationTimeout: 2s\nObservationPeriod: 40s\n"), &overridden); err != nil {
		t.Fatal(err)
	}
	if overridden != (UnboundRecoveryConf{ScanInterval: 20 * time.Second, PageSize: 257, RunTimeout: 8 * time.Second, OperationTimeout: 2 * time.Second, ObservationPeriod: 40 * time.Second}) || overridden.Validate() != nil {
		t.Fatal("deployment values were clamped or ignored")
	}
}

func TestPrepareExecutionRejectsInvalidUnboundRecovery(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*UnboundRecoveryConf)
	}{
		{"interval", func(c *UnboundRecoveryConf) { c.ScanInterval = 0 }},
		{"interval overflow", func(c *UnboundRecoveryConf) { c.ScanInterval = time.Duration(1<<63 - 1) }},
		{"page", func(c *UnboundRecoveryConf) { c.PageSize = -1 }},
		{"run", func(c *UnboundRecoveryConf) { c.RunTimeout = 0 }},
		{"operation", func(c *UnboundRecoveryConf) { c.OperationTimeout = -time.Second }},
		{"observation", func(c *UnboundRecoveryConf) { c.ObservationPeriod = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := validExecutionConfig()
			test.edit(&c.UnboundRecovery)
			if err := c.PrepareExecution(); err == nil {
				t.Fatal("invalid recovery setting was accepted")
			}
		})
	}
}
