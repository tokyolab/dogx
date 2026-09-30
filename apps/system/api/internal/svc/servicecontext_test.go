package svc

import (
	"github.com/tokyolab/dogx/apps/system/api/internal/config"
	"strings"
	"testing"
)

func TestServiceContextCloseWithoutDependencies(t *testing.T) {
	if err := (&ServiceContext{}).Close(); err != nil {
		t.Fatalf("close empty service context: %v", err)
	}
}

func TestInvalidTrustedProxiesRejectStartup(t *testing.T) {
	sc, err := NewServiceContext(config.Config{Auth: config.AuthConf{TrustedProxies: []string{"127.0.0.1/32", "invalid"}}})
	if err == nil || sc != nil || !strings.Contains(err.Error(), "trusted proxies") {
		t.Fatalf("invalid proxy configuration did not stop startup: %v", err)
	}
}
