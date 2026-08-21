package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	App       AppConf
	Postgres  PostgresConf
	RedisConf redis.RedisConf
}

type AppConf struct {
	ReadinessTimeout time.Duration `json:",default=2s"`
}

type PostgresConf struct {
	Host            string
	Port            int `json:",default=5432"`
	User            string
	Password        string
	Database        string
	SSLMode         string        `json:",default=disable,options=disable|require|verify-ca|verify-full"`
	TimeZone        string        `json:",default=Asia/Shanghai"`
	MaxIdleConns    int           `json:",default=10,range=[0:1000]"`
	MaxOpenConns    int           `json:",default=50,range=[1:10000]"`
	ConnMaxLifetime time.Duration `json:",default=1h"`
}

func (c PostgresConf) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=%s",
		quoteDSNValue(c.Host),
		c.Port,
		quoteDSNValue(c.User),
		quoteDSNValue(c.Password),
		quoteDSNValue(c.Database),
		c.SSLMode,
		c.TimeZone,
	)
}

func quoteDSNValue(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return `'` + escaped + `'`
}
