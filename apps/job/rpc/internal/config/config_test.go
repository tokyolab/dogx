package config

import (
	"github.com/zeromicro/go-zero/core/conf"
	"testing"
	"time"
)

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
