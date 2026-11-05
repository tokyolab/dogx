package config

import (
	"fmt"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestInstanceCheckDeploymentConfiguration(t *testing.T) {
	var defaults InstanceCheckConf
	if err := conf.LoadFromYamlBytes([]byte("{}"), &defaults); err != nil {
		t.Fatal(err)
	}
	if defaults != (InstanceCheckConf{Capacity: 512, Concurrency: 8, Timeout: 2 * time.Second}) || defaults.Validate() != nil {
		t.Fatalf("unexpected instance check defaults: %+v", defaults)
	}
	for _, field := range []string{"Capacity", "Concurrency", "Timeout"} {
		for _, input := range []string{"0", "-1"} {
			t.Run(field+input, func(t *testing.T) {
				var value InstanceCheckConf
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
	c.InstanceCheck.Timeout = 0
	if err := c.PrepareExecution(); err == nil {
		t.Fatal("invalid check budget passed host validation")
	}
}
