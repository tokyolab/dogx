package config

import (
	"fmt"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestDispatchDeploymentDefaults(t *testing.T) {
	var value DispatchConf
	if err := conf.LoadFromYamlBytes([]byte("{}"), &value); err != nil {
		t.Fatal(err)
	}
	want := DispatchConf{Concurrency: 64, WaitingCapacity: 256, ScanInterval: time.Second,
		TotalTimeout: 10 * time.Second, RPCTimeout: 3 * time.Second, BusyDelay: 5 * time.Second}
	if value != want || value.Validate() != nil {
		t.Fatalf("dispatch defaults=%+v", value)
	}
}

func TestDispatchRejectsExplicitInvalidDeploymentValues(t *testing.T) {
	for _, field := range []string{"Concurrency", "WaitingCapacity", "ScanInterval", "TotalTimeout", "RPCTimeout", "BusyDelay"} {
		for _, input := range []string{"0", "-1"} {
			t.Run(field+input, func(t *testing.T) {
				var value DispatchConf
				err := conf.LoadFromYamlBytes([]byte(fmt.Sprintf("%s: %s\n", field, input)), &value)
				if err == nil {
					err = value.Validate()
				}
				if err == nil {
					t.Fatal("explicit invalid setting received a default")
				}
			})
		}
	}
	c := validExecutionConfig()
	c.Dispatch.RPCTimeout = time.Microsecond
	if err := c.PrepareExecution(); err == nil {
		t.Fatal("native millisecond timeout truncation was accepted")
	}
	c = validExecutionConfig()
	c.Dispatch.BusyDelay = time.Duration(1<<63 - 1)
	if err := c.PrepareExecution(); err == nil {
		t.Fatal("overflowing jitter was accepted")
	}
}
