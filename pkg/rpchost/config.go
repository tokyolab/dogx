package rpchost

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/service"
)

// ShutdownConfig is the process-wide shutdown policy read from Shutdown in the
// host configuration. WaitTime includes WrapUpTime and the remaining drain time;
// it is not a separate allowance for each server or background component.
// Windows uses the same policy even though go-zero's native ShutdownConf is empty.
type ShutdownConfig struct {
	WrapUpTime time.Duration `json:",default=1s"`
	WaitTime   time.Duration `json:",default=30s"`
}

// LoadConfig reads one configuration snapshot, expands environment variables
// once, and decodes both the application's native configuration and its shared
// shutdown policy. It accepts the same YAML, JSON, JSON5 and TOML file extensions
// as go-zero. The target must be a non-nil pointer to a struct and is usable only
// after success; a failed application decode may have partially populated it.
//
// The shutdown projection uses the existing Shutdown key instead of shadowing
// RpcServerConf's embedded field, which go-zero rejects as a conflicting key.
// Normalize must be called before constructing services to synchronize native
// process settings. Decoding errors omit configuration values because even a
// malformed value can contain a credential.
func LoadConfig(file string, v any) (ShutdownConfig, error) {
	target := reflect.ValueOf(v)
	if !target.IsValid() || target.Kind() != reflect.Pointer || target.IsNil() || target.Elem().Kind() != reflect.Struct {
		return ShutdownConfig{}, errors.New("configuration target must be a non-nil pointer to a struct")
	}
	var load func([]byte, any) error
	switch strings.ToLower(filepath.Ext(file)) {
	case ".yaml", ".yml":
		load = conf.LoadFromYamlBytes
	case ".json":
		load = conf.LoadFromJsonBytes
	case ".json5":
		load = conf.LoadFromJson5Bytes
	case ".toml":
		load = conf.LoadFromTomlBytes
	default:
		return ShutdownConfig{}, errors.New("unsupported configuration file type")
	}
	content, err := os.ReadFile(file)
	if err != nil {
		return ShutdownConfig{}, fmt.Errorf("read process configuration: %w", err)
	}
	content = []byte(os.ExpandEnv(string(content)))
	var projection struct {
		// An optional outer field would suppress nested defaults when absent.
		Shutdown ShutdownConfig
	}
	if err := load(content, &projection); err != nil {
		return ShutdownConfig{}, errors.New("invalid process shutdown configuration")
	}
	if err := projection.Shutdown.validate(); err != nil {
		return ShutdownConfig{}, err
	}
	if err := load(content, v); err != nil {
		return ShutdownConfig{}, errors.New("invalid application configuration")
	}
	return projection.Shutdown, nil
}

// Normalize copies this policy into the supplied host-owned ServiceConf where
// go-zero supports native shutdown settings. It validates before changing the
// configuration and does not initialize global services or install signal
// handlers. All servers in the process must receive the resulting ServiceConf.
// On Windows the native configuration is empty; the host still uses this policy
// to coordinate its own signal handling and shared shutdown deadline.
func (c ShutdownConfig) Normalize(config *service.ServiceConf) error {
	if config == nil {
		return errors.New("host service configuration is required")
	}
	if err := c.validate(); err != nil {
		return err
	}
	normalizeShutdown(config, c)
	return nil
}

func (c ShutdownConfig) validate() error {
	if c.WrapUpTime <= 0 || c.WaitTime <= c.WrapUpTime {
		return errors.New("shutdown WrapUpTime must be positive and less than WaitTime")
	}
	return nil
}
