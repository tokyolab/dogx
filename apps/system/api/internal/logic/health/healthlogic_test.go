package health

import (
	"context"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
)

func TestHealth(t *testing.T) {
	svcCtx := &svc.ServiceContext{Config: config.Config{App: config.AppConf{Version: "v0.1.0"}}}
	svcCtx.Config.Name = "system-api"

	response, err := NewHealthLogic(context.Background(), svcCtx).Health()
	if err != nil {
		t.Fatalf("health returned error: %v", err)
	}
	if response.Status != "ok" || response.Service != "system-api" || response.Version != "v0.1.0" {
		t.Fatalf("unexpected response: %+v", response)
	}
}
