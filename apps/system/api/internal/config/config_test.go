package config

import (
	"path/filepath"
	"testing"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestExampleConfigLoads(t *testing.T) {
	var loaded Config
	path := filepath.Join("..", "..", "etc", "system-api.example.yaml")
	if err := conf.Load(path, &loaded, conf.UseEnv()); err != nil {
		t.Fatalf("load example config: %v", err)
	}

	if loaded.Name != "system-api" || loaded.Port != 8001 {
		t.Fatalf("unexpected REST config: name=%s port=%d", loaded.Name, loaded.Port)
	}
	if len(loaded.SystemRpc.Endpoints) != 1 || loaded.SystemRpc.Endpoints[0] != "127.0.0.1:9001" {
		t.Fatalf("unexpected system RPC endpoints: %v", loaded.SystemRpc.Endpoints)
	}
}
