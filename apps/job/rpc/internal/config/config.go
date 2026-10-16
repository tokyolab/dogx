package config

import (
	"errors"
	"time"

	"github.com/tokyolab/dogx/pkg/postgres"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Postgres  postgres.Config
	Scheduler SchedulerConf
}

type SchedulerConf struct {
	Prefix              string
	QueueCapacity       int           `json:",default=1024"`
	CreationConcurrency int           `json:",default=32"`
	CreationBudget      time.Duration `json:",default=3s"`
	LeaseTTLSeconds     int           `json:",default=15"`
	RecoveryLimit       int           `json:",default=1024"`
	QueryTimeout        time.Duration `json:",default=5s"`
}

func (c SchedulerConf) Validate() error {
	if c.QueueCapacity <= 0 || c.CreationConcurrency <= 0 || c.CreationBudget <= 0 || c.LeaseTTLSeconds <= 0 || c.RecoveryLimit <= 0 || c.QueryTimeout <= 0 {
		return errors.New("scheduler capacities and budgets must be positive")
	}
	return nil
}
