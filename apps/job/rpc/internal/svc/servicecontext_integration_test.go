//go:build integration

package svc

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	"github.com/tokyolab/dogx/apps/job/internal/testutil"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/config"
	"github.com/tokyolab/dogx/pkg/postgres"
	pgtest "github.com/tokyolab/dogx/pkg/testutil/postgres"
	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/zrpc"
)

func jobServiceConfig(t *testing.T, db *sql.DB) (config.Config, string) {
	t.Helper()
	parsed, err := pgx.ParseConfig(pgtest.PostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := db.QueryRow("SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(schema, "dogx_it_") {
		t.Fatalf("refuse to wire service context to unexpected schema %q", schema)
	}
	// Production opens its own pool. Use PostgreSQL's connection environment to
	// keep that pool in this test's schema and identify its lifecycle separately.
	applicationName := "dogx-job-svc-" + schema
	t.Setenv("PGOPTIONS", "-c search_path="+schema)
	t.Setenv("PGAPPNAME", applicationName)
	t.Setenv("PGCONNECT_TIMEOUT", "1")
	return config.Config{
		RpcServerConf: zrpc.RpcServerConf{Etcd: discov.EtcdConf{Hosts: []string{"127.0.0.1:1"}, Key: "/dogx/test/job"}},
		Postgres: postgres.Config{Host: parsed.Host, Port: int(parsed.Port), User: parsed.User, Password: parsed.Password, Database: parsed.Database,
			SSLMode: "disable", TimeZone: "Asia/Shanghai", MaxIdleConns: 1, MaxOpenConns: 2, ConnMaxLifetime: time.Minute},
		Scheduler: config.SchedulerConf{Prefix: "/dogx/test/" + schema, QueueCapacity: 1024, CreationConcurrency: 32, CreationBudget: 3 * time.Second, LeaseTTLSeconds: 15, RecoveryLimit: 1024, QueryTimeout: 5 * time.Second},
	}, applicationName
}

func TestNewServiceContextWiresJobsAndClosesOfflineElection(t *testing.T) {
	db, sqlDB := testutil.OpenMigratedPostgres(t)
	c, applicationName := jobServiceConfig(t, sqlDB)
	s, err := NewServiceContext(c)
	if err != nil {
		t.Fatalf("initialize Job service with offline etcd: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close Job service: %v", err)
		}
	})
	if s.Jobs == nil || s.etcd == nil || s.sqlDB == nil || s.done == nil || s.cancel == nil {
		t.Fatal("service dependencies are incomplete")
	}
	if s.sqlDB.Stats().MaxOpenConnections != c.Postgres.MaxOpenConns {
		t.Fatalf("pool limits=%+v", s.sqlDB.Stats())
	}
	var actualApplication string
	if err := s.sqlDB.QueryRow("SELECT current_setting('application_name')").Scan(&actualApplication); err != nil || actualApplication != applicationName {
		t.Fatalf("service connection identity=%q error=%v", actualApplication, err)
	}
	group := model.ExecutorGroup{Name: "offline executor group"}
	if err := db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := s.Jobs.CreateJob(ctx, &model.JobDefinition{Name: "wired job", ExecutorGroupID: group.ID, ProcessorName: "Handler", ExecutionMode: model.ExecutionModeNormal, Params: "payload", CronExpr: "0 * * * * *", Status: model.StatusDisabled, RetryIntervalSeconds: 10, TimeoutSeconds: 900})
	if err != nil || id <= 0 {
		t.Fatalf("create through wired service: id=%d error=%v", id, err)
	}
	detail, err := s.Jobs.GetJob(ctx, id)
	if err != nil || detail.Name != "wired job" || detail.ExecutorGroupName == nil || *detail.ExecutorGroupName != group.Name {
		t.Fatalf("get through wired service: detail=%+v error=%v", detail, err)
	}
	items, total, err := s.Jobs.ListJobs(ctx, repository.JobFilter{Page: 1, PageSize: 20})
	if err != nil || total != 1 || len(items) != 1 || items[0].ID != id {
		t.Fatalf("list through wired service: total=%d items=%+v error=%v", total, items, err)
	}
	instanceID, err := s.Jobs.TriggerManual(ctx, service.ManualTrigger{JobID: id, RequestID: uuid.MustParse("12051a0f-76b8-4d88-897f-15ec600de323"), OperatorID: 42})
	if err != nil || instanceID <= 0 {
		t.Fatalf("manual trigger through wired service: id=%d error=%v", instanceID, err)
	}
	var instance model.JobInstance
	if err := db.First(&instance, instanceID).Error; err != nil {
		t.Fatal(err)
	}
	if instance.Status != model.InstanceWaiting || instance.StartedAt != nil || instance.DeadlineAt != nil || instance.ExecutionConfig.Params != "payload" {
		t.Fatalf("unexpected handoff=%+v", instance)
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing an offline election did not finish")
	}
	select {
	case <-s.done:
	default:
		t.Fatal("Close returned before scheduler stopped")
	}
	if s.etcd.Ctx().Err() == nil {
		t.Fatal("etcd client was not closed")
	}
	if err := s.sqlDB.PingContext(context.Background()); err == nil {
		t.Fatal("PostgreSQL pool remained open")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	assertNoServiceConnections(t, sqlDB, applicationName)
}

func TestNewServiceContextInitializationFailuresReleaseConnections(t *testing.T) {
	_, sqlDB := testutil.OpenMigratedPostgres(t)
	base, applicationName := jobServiceConfig(t, sqlDB)
	for _, test := range []struct {
		name, contains string
		change         func(*config.Config)
	}{
		{"discovery", "job discovery", func(c *config.Config) { c.Etcd.Hosts = nil }},
		{"scheduler capacity", "scheduler capacities", func(c *config.Config) { c.Scheduler.CreationConcurrency = 0 }},
		{"TLS files", "etcd TLS", func(c *config.Config) {
			dir := t.TempDir()
			c.Etcd.CertFile = filepath.Join(dir, "missing.crt")
			c.Etcd.CertKeyFile = filepath.Join(dir, "missing.key")
			c.Etcd.CACertFile = filepath.Join(dir, "missing-ca.crt")
		}},
		{"database", "connect postgres", func(c *config.Config) { c.Postgres.Host = "127.0.0.1"; c.Postgres.Port = 1 }},
		{"scheduler prefix", "scheduler prefix", func(c *config.Config) { c.Scheduler.Prefix = "relative" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := base
			test.change(&c)
			s, err := NewServiceContext(c)
			if s != nil {
				_ = s.Close()
				t.Fatal("failed constructor returned a service context")
			}
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("unexpected initialization error: %v", err)
			}
			assertNoServiceConnections(t, sqlDB, applicationName)
		})
	}
}

func assertNoServiceConnections(t *testing.T, db *sql.DB, applicationName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1", applicationName).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%d Job service PostgreSQL connections remained after cleanup", count)
		case <-ticker.C:
		}
	}
}
