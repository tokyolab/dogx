package authorization

import (
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestGormAdapterBoundsPolicyInsertBatches(t *testing.T) {
	tests := []struct {
		name       string
		policies   int
		parameters []int
	}{
		{name: "single policy", policies: 1, parameters: []int{7}},
		{name: "full batch", policies: 5000, parameters: []int{35000}},
		{name: "batch plus one", policies: 5001, parameters: []int{35000, 7}},
		{name: "previous protocol overflow", policies: 9363, parameters: []int{35000, 30541}},
		{name: "maximum requested APIs", policies: 10000, parameters: []int{35000, 35000}},
		{name: "maximum plus required API", policies: 10001, parameters: []int{35000, 35000, 7}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// This only generates PostgreSQL SQL. Transaction behavior is covered
			// separately by the real-database integration tests.
			db, err := gorm.Open(postgres.New(postgres.Config{
				DSN: "host=127.0.0.1 port=1 user=unused dbname=unused sslmode=disable",
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				DryRun:                 true,
				SkipDefaultTransaction: true,
			})
			if err != nil {
				t.Fatalf("create dry-run database: %v", err)
			}
			pool, err := db.DB()
			if err != nil {
				t.Fatalf("get dry-run pool: %v", err)
			}
			t.Cleanup(func() {
				if err := pool.Close(); err != nil {
					t.Errorf("close dry-run pool: %v", err)
				}
			})
			var parameters []int
			if err := db.Callback().Create().After("gorm:create").Register("test:policy_parameters", func(tx *gorm.DB) {
				parameters = append(parameters, len(tx.Statement.Vars))
				_, err := (&pgproto3.Bind{Parameters: make([][]byte, len(tx.Statement.Vars))}).Encode(nil)
				if err != nil {
					t.Errorf("policy batch exceeds PostgreSQL protocol limit: %v", err)
				}
			}); err != nil {
				t.Fatalf("register parameter check: %v", err)
			}
			ctx := t.Context()
			db = db.WithContext(ctx)
			adapter, err := NewGormAdapter(db)
			if err != nil {
				t.Fatalf("create adapter: %v", err)
			}
			if db.CreateBatchSize != 0 || db.Statement.Context != ctx {
				t.Fatal("adapter configuration mutated the business database handle")
			}
			if adapter.GetDb().Statement.ConnPool != db.Statement.ConnPool {
				t.Fatal("adapter lost the supplied connection pool")
			}
			rules := make([][]string, test.policies)
			for index := range rules {
				rules[index] = []string{"r:2", fmt.Sprintf("/batch/%05d", index), "POST"}
			}
			if err := adapter.AddPoliciesCtx(ctx, "p", "p", rules); err != nil {
				t.Fatalf("generate policy inserts: %v", err)
			}
			if !slices.Equal(parameters, test.parameters) {
				t.Fatalf("parameters per insert = %v, want %v", parameters, test.parameters)
			}
		})
	}
}
