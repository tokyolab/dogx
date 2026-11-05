package repository

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

func TestTaskRepositoryRejectsNilDatabase(t *testing.T) {
	if repo, err := NewJobTaskRepository(nil); repo != nil || err == nil {
		t.Fatalf("nil database: repository = %v, error = %v", repo, err)
	}
}

func TestTaskRepositoryRejectsInvalidInputsBeforeDatabaseAccess(t *testing.T) {
	repo, err := NewJobTaskRepository(&gorm.DB{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	started := time.Date(2026, 11, 4, 1, 2, 3, 0, time.UTC)
	for _, tc := range []struct {
		id int64
		at time.Time
	}{{0, started}, {-1, started}, {1, time.Time{}}} {
		if id, err := repo.InsertOrdinary(ctx, tc.id, tc.at); id != 0 || err == nil {
			t.Fatalf("invalid start: id = %d, error = %v", id, err)
		}
	}
	valid := ExecutorBinding{TaskID: 1, InstanceID: 2, AttemptNo: 1,
		ExecutorInstanceID: uuid.MustParse("11111111-1111-4111-8111-111111111111"), ExecutorAddress: "127.0.0.1:8080"}
	for name, mutate := range map[string]func(*ExecutorBinding){
		"zero task":        func(b *ExecutorBinding) { b.TaskID = 0 },
		"negative task":    func(b *ExecutorBinding) { b.TaskID = -1 },
		"zero instance":    func(b *ExecutorBinding) { b.InstanceID = 0 },
		"negative attempt": func(b *ExecutorBinding) { b.AttemptNo = -1 },
		"zero attempt":     func(b *ExecutorBinding) { b.AttemptNo = 0 },
		"nil identity":     func(b *ExecutorBinding) { b.ExecutorInstanceID = uuid.Nil },
		"wrong version":    func(b *ExecutorBinding) { b.ExecutorInstanceID[6] = 0x50 },
		"wrong variant":    func(b *ExecutorBinding) { b.ExecutorInstanceID[8] = 0 },
		"bad address":      func(b *ExecutorBinding) { b.ExecutorAddress = "127.0.0.1" },
	} {
		t.Run(name, func(t *testing.T) {
			binding := valid
			mutate(&binding)
			if applied, err := repo.BindExecutor(ctx, binding); applied || err == nil {
				t.Fatalf("invalid binding: applied = %v, error = %v", applied, err)
			}
		})
	}
}

func TestExecutorAddressStructure(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8080", "worker.internal:65535", "[::1]:1", "[fe80::1%eth0]:9090", strings.Repeat("a", 253) + ":1"} {
		if !validExecutorAddress(address) {
			t.Errorf("rejected structurally valid address %q", address)
		}
	}
	for _, address := range []string{
		"", ":8080", "127.0.0.1", "127.0.0.1:", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:-1",
		"127.0.0.1:+80", "127.0.0.1:http", "127.0.0.1:80 ", "127.0.0.1:８０", " bad:80", "bad\n:80",
		"bad\x00:80", "bad\xff:80", "http://worker:80", "worker/path:80", "worker?query:80", "user@worker:80",
		"::1:80", "[worker]:80", "[bad::ip]:80", "[::1%]:80", strings.Repeat("a", 254) + ":1",
	} {
		if validExecutorAddress(address) {
			t.Errorf("accepted invalid address %q", address)
		}
	}
}
