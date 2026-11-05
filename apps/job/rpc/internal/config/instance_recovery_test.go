package config

import (
	"fmt"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestInstanceRecoveryDeploymentConfiguration(t *testing.T) {
	var defaults InstanceRecoveryConf
	if err := conf.LoadFromYamlBytes([]byte("{}"), &defaults); err != nil {
		t.Fatal(err)
	}
	if defaults != (InstanceRecoveryConf{Interval: 2 * time.Minute, PageSize: 256, RunTimeout: 5 * time.Second, SQLTimeout: time.Second}) || defaults.Validate() != nil {
		t.Fatalf("unexpected instance recovery defaults: %+v", defaults)
	}
	for _, field := range []string{"Interval", "PageSize", "RunTimeout", "SQLTimeout"} {
		for _, input := range []string{"0", "-1"} {
			var value InstanceRecoveryConf
			err := conf.LoadFromYamlBytes([]byte(fmt.Sprintf("%s: %s\n", field, input)), &value)
			if err == nil {
				err = value.Validate()
			}
			if err == nil {
				t.Fatalf("invalid %s received a default", field)
			}
		}
	}
	c := validExecutionConfig()
	c.InstanceRecovery.SQLTimeout = 0
	if c.PrepareExecution() == nil {
		t.Fatal("invalid recovery budget passed host validation")
	}
}
