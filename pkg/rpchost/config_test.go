package rpchost

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
)

type configProbe struct {
	zrpc.RpcServerConf
	Marker string `json:",optional"`
}

func TestLoadConfigShutdownDefaultsAndOverrides(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want ShutdownConfig
	}{
		{"omitted", "", ShutdownConfig{WrapUpTime: time.Second, WaitTime: 30 * time.Second}},
		{"empty", "Shutdown: {}\n", ShutdownConfig{WrapUpTime: time.Second, WaitTime: 30 * time.Second}},
		{"wait override", "Shutdown:\n  WaitTime: 45s\n", ShutdownConfig{WrapUpTime: time.Second, WaitTime: 45 * time.Second}},
		{"wrap override", "Shutdown:\n  WrapUpTime: 2s\n", ShutdownConfig{WrapUpTime: 2 * time.Second, WaitTime: 30 * time.Second}},
		{"both", "Shutdown:\n  WrapUpTime: 3s\n  WaitTime: 50s\n", ShutdownConfig{WrapUpTime: 3 * time.Second, WaitTime: 50 * time.Second}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var app configProbe
			file := writeConfigProbe(t, ".yaml", "Name: shutdown-probe\nListenOn: 127.0.0.1:0\n"+test.body)
			got, err := LoadConfig(file, &app)
			if err != nil || got != test.want {
				t.Fatalf("shutdown = %+v, error = %v, want %+v", got, err, test.want)
			}
			if app.Name != "shutdown-probe" || app.ListenOn != "127.0.0.1:0" || app.Timeout != 2000 {
				t.Fatal("native RPC configuration was not populated with its normal defaults")
			}
			if err := got.Normalize(&app.ServiceConf); err != nil {
				t.Fatal(err)
			}
			assertNormalizedShutdown(t, app.ServiceConf, got)
		})
	}
}

func TestLoadConfigSupportedFormats(t *testing.T) {
	for _, test := range []struct{ extension, content string }{
		{".yaml", "Name: shutdown-probe\nListenOn: 127.0.0.1:0\n"},
		{".yml", "Name: shutdown-probe\nListenOn: 127.0.0.1:0\n"},
		{".YAML", "Name: shutdown-probe\nListenOn: 127.0.0.1:0\n"},
		{".json", `{"Name":"shutdown-probe","ListenOn":"127.0.0.1:0"}`},
		{".json5", "{\n // Synthetic configuration.\n Name: 'shutdown-probe', ListenOn: '127.0.0.1:0',\n}"},
		{".toml", "Name = 'shutdown-probe'\nListenOn = '127.0.0.1:0'\n"},
	} {
		t.Run(test.extension, func(t *testing.T) {
			var app configProbe
			got, err := LoadConfig(writeConfigProbe(t, test.extension, test.content), &app)
			if err != nil || got.WaitTime != 30*time.Second || app.Name != "shutdown-probe" {
				t.Fatalf("load supported format: %+v, %v", got, err)
			}
		})
	}
}

func TestLoadConfigExpandsEnvironmentOnce(t *testing.T) {
	t.Setenv("DOGX_SHUTDOWN_PROBE_WAIT", "41s")
	t.Setenv("DOGX_SHUTDOWN_PROBE_MARKER", "$DOGX_SHUTDOWN_PROBE_NESTED")
	t.Setenv("DOGX_SHUTDOWN_PROBE_NESTED", "must-not-be-expanded-again")
	file := writeConfigProbe(t, ".yaml", "Name: shutdown-probe\nListenOn: 127.0.0.1:0\nMarker: ${DOGX_SHUTDOWN_PROBE_MARKER}\nShutdown:\n  WaitTime: ${DOGX_SHUTDOWN_PROBE_WAIT}\n")
	var app configProbe
	got, err := LoadConfig(file, &app)
	if err != nil || got.WaitTime != 41*time.Second || app.Marker != "$DOGX_SHUTDOWN_PROBE_NESTED" {
		t.Fatalf("environment expansion did not use one shared snapshot: %+v, %v", got, err)
	}
}

func TestLoadConfigRejectsInvalidShutdownWithoutValues(t *testing.T) {
	const sensitive = "synthetic-sensitive-value"
	for _, body := range []string{
		"Shutdown:\n  WaitTime: 0s\n", "Shutdown:\n  WaitTime: -1s\n",
		"Shutdown:\n  WrapUpTime: 0s\n", "Shutdown:\n  WrapUpTime: -1s\n",
		"Shutdown:\n  WrapUpTime: 30s\n", "Shutdown:\n  WrapUpTime: 31s\n",
		"Shutdown: null\n", "Shutdown:\n  WaitTime: " + sensitive + "\n",
	} {
		var app configProbe
		file := writeConfigProbe(t, ".yaml", "Name: shutdown-probe\nListenOn: 127.0.0.1:0\nMarker: "+sensitive+"\n"+body)
		got, err := LoadConfig(file, &app)
		if err == nil || got != (ShutdownConfig{}) || strings.Contains(err.Error(), sensitive) {
			t.Fatal("invalid shutdown configuration was accepted or disclosed a value")
		}
		if app.Name != "" {
			t.Fatal("application decoding ran before shutdown validation")
		}
	}
	var app configProbe
	file := writeConfigProbe(t, ".yaml", "Name: shutdown-probe\nListenOn: 127.0.0.1:0\nTimeout: "+sensitive+"\n")
	if _, err := LoadConfig(file, &app); err == nil || strings.Contains(err.Error(), sensitive) {
		t.Fatal("invalid application configuration was accepted or disclosed a value")
	}
}

func TestLoadConfigRejectsInvalidTargetAndFile(t *testing.T) {
	file := writeConfigProbe(t, ".yaml", "Name: shutdown-probe\nListenOn: 127.0.0.1:0\n")
	var nilConfig *configProbe
	for _, target := range []any{nil, nilConfig, configProbe{}, new(string)} {
		if _, err := LoadConfig(file, target); err == nil {
			t.Fatal("invalid target was accepted")
		}
	}
	var app configProbe
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "missing.yaml"), &app); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing file error did not preserve its filesystem cause")
	}
	if _, err := LoadConfig(writeConfigProbe(t, ".txt", "synthetic config"), &app); err == nil {
		t.Fatal("unsupported extension was accepted")
	}
}

func TestShutdownNormalizeValidatesBeforeMutation(t *testing.T) {
	config := service.ServiceConf{Name: "shutdown-probe"}
	valid := ShutdownConfig{WrapUpTime: 2 * time.Second, WaitTime: 40 * time.Second}
	if err := valid.Normalize(&config); err != nil {
		t.Fatal(err)
	}
	before := config
	if err := (ShutdownConfig{}).Normalize(&config); err == nil || !reflect.DeepEqual(config, before) {
		t.Fatal("invalid policy mutated native service configuration")
	}
	if err := valid.Normalize(nil); err == nil {
		t.Fatal("nil service configuration was accepted")
	}
	assertNormalizedShutdown(t, config, valid)
}

func writeConfigProbe(t *testing.T, extension, content string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "config"+extension)
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func assertNormalizedShutdown(t *testing.T, config service.ServiceConf, want ShutdownConfig) {
	t.Helper()
	native := reflect.ValueOf(config.Shutdown)
	if runtime.GOOS == "windows" {
		if native.NumField() != 0 {
			t.Fatal("review the normalization adapter after native Windows shutdown support changes")
		}
		return
	}
	for name, expected := range map[string]time.Duration{"WaitTime": want.WaitTime, "WrapUpTime": want.WrapUpTime} {
		field := native.FieldByName(name)
		if !field.IsValid() || time.Duration(field.Int()) != expected {
			t.Fatalf("native %s does not match the shared process policy", name)
		}
	}
}
