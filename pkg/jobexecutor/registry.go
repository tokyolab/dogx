package jobexecutor

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode/utf8"
)

var (
	// ErrInvalidRegistration reports an invalid name or a nil processor.
	ErrInvalidRegistration = errors.New("invalid processor registration")
	// ErrProcessorExists reports a name used by either kind of processor.
	ErrProcessorExists = errors.New("processor name is already registered")
	// ErrRegistryFrozen reports an attempted mutation of a runtime registry.
	ErrRegistryFrozen = errors.New("processor registry is frozen")
)

// Registry maps exact, case-sensitive names to ordinary or sharding processors.
// The two registration kinds share one namespace and never replace an entry.
// Its zero value is usable. Methods are safe for concurrent calls, but a Registry
// must not be copied after first use. The host registers its processors and calls
// Freeze before admitting execution requests; registration then remains fixed.
type Registry struct {
	mu       sync.RWMutex
	frozen   bool
	ordinary map[string]Processor
	sharding map[string]ShardingProcessor
}

// NewRegistry returns an empty, mutable processor registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds an ordinary processor. Names must match the Job processor name:
// nonempty, valid UTF-8, no NUL or surrounding whitespace, and at most 128
// Unicode characters. It does not normalize or truncate names. A nil interface
// or typed nil is rejected. The same name cannot be registered as sharding work.
func (r *Registry) Register(name string, processor Processor) error {
	if err := validateRegistration(name, processor); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkName(name); err != nil {
		return err
	}
	if r.ordinary == nil {
		r.ordinary = make(map[string]Processor)
	}
	r.ordinary[name] = processor
	return nil
}

// RegisterSharding adds preparation and shard execution under one name, using
// the same validation and namespace as Register. An optional PostProcessor is
// implemented by this same object; it is not registered under a separate name.
func (r *Registry) RegisterSharding(name string, processor ShardingProcessor) error {
	if err := validateRegistration(name, processor); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkName(name); err != nil {
		return err
	}
	if r.sharding == nil {
		r.sharding = make(map[string]ShardingProcessor)
	}
	r.sharding[name] = processor
	return nil
}

// Freeze prevents future registrations and is safe to call repeatedly.
// Concurrent registration either finishes before Freeze or receives
// ErrRegistryFrozen; a frozen registry never exposes a partial registration.
func (r *Registry) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

// Lookup returns an ordinary processor by its exact name. A missing or sharding
// name returns (nil, false); a successful lookup always returns a non-nil object.
func (r *Registry) Lookup(name string) (Processor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	processor, ok := r.ordinary[name]
	return processor, ok
}

// LookupSharding returns a sharding processor by its exact name. A missing or
// ordinary name returns (nil, false). The caller can assert PostProcessor on the
// returned object to check optional finalization capability without invoking it.
func (r *Registry) LookupSharding(name string) (ShardingProcessor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	processor, ok := r.sharding[name]
	return processor, ok
}

// checkName requires the write lock so the frozen and duplicate checks are
// atomic with registration, including competition between the two kinds.
func (r *Registry) checkName(name string) error {
	if r.frozen {
		return ErrRegistryFrozen
	}
	if _, ok := r.ordinary[name]; ok {
		return ErrProcessorExists
	}
	if _, ok := r.sharding[name]; ok {
		return ErrProcessorExists
	}
	return nil
}

func validateRegistration(name string, processor any) error {
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, 0) ||
		name != strings.TrimSpace(name) || utf8.RuneCountInString(name) > 128 {
		return fmt.Errorf("%w: processor name is invalid", ErrInvalidRegistration)
	}
	if processor == nil {
		return fmt.Errorf("%w: processor must not be nil", ErrInvalidRegistration)
	}
	value := reflect.ValueOf(processor)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return fmt.Errorf("%w: processor must not be nil", ErrInvalidRegistration)
		}
	}
	return nil
}
