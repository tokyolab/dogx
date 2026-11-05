// Package groupcache maintains each scheduler process's executor-group
// credentials. It does not discover executor addresses or authorize Task work.
package groupcache

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/zeromicro/go-zero/core/logx"
)

const refreshInterval = 60 * time.Second

// ErrNotFound means a successful source lookup found no active group. A cache
// miss or a failed database query alone must not produce this error.
var ErrNotFound = errors.New("executor group was not found")

// Source reads active groups from the primary database. It must support
// concurrent calls and honor their Contexts. A lookup returns either a complete
// non-nil configuration or an error; missing and deleted groups return
// repository.ErrExecutorGroupUnavailable. LoadExecutionGroups returns only a
// complete snapshot, never partial data with a nil error. The cache owns copies
// of returned values; callers must not modify a returned slice concurrently.
type Source interface {
	LoadExecutionGroups(context.Context) ([]repository.ExecutorGroupConfig, error)
	FindExecutionGroupByID(context.Context, int64) (*repository.ExecutorGroupConfig, error)
	FindExecutionGroupByName(context.Context, string) (*repository.ExecutorGroupConfig, error)
}

// Cache stores complete, immutable group values indexed by ID and exact name.
// Reads and full replacement are atomic with respect to miss publication. All
// public operations are safe for concurrent use. A failed refresh retains the
// last successful snapshot; the refresh interval is not a credential TTL or a
// promise of immediate deletion visibility.
//
// Cache creates no goroutines and owns no database connections. The owner runs
// Run, cancels its Context during shutdown, and waits for it before closing the
// source. It must also wait for request lookups to finish. Cancellation bounds
// database work only when Source honors the Context; it is not a completion
// signal for an implementation that ignores cancellation.
type Cache struct {
	source       Source
	queryTimeout time.Duration
	refreshGate  chan struct{}
	runStarted   atomic.Bool

	mu           sync.RWMutex
	generation   uint64
	byID         map[int64]repository.ExecutorGroupConfig
	byName       map[string]repository.ExecutorGroupConfig
	flights      map[lookupKey]*lookupFlight
	observers    map[uint64]func()
	nextObserver uint64
}

type lookupKey struct {
	id   int64
	name string
}

// lookupFlight shares one source call among equal lookup keys. The initiating
// caller executes it synchronously; followers may stop waiting independently.
// Its result becomes immutable before done is closed.
type lookupFlight struct {
	done   chan struct{}
	config repository.ExecutorGroupConfig
	err    error
}

// New performs the first complete load before returning a usable Cache. Empty
// databases are valid; source failures and invalid stored configurations are
// not. queryTimeout must be positive and bounds each source call in addition to
// the caller's deadline. A failed New starts no background work.
func New(ctx context.Context, source Source, queryTimeout time.Duration) (*Cache, error) {
	if source == nil || (reflect.ValueOf(source).Kind() == reflect.Pointer && reflect.ValueOf(source).IsNil()) {
		return nil, errors.New("executor group cache source is nil")
	}
	if queryTimeout <= 0 {
		return nil, errors.New("executor group cache query timeout must be positive")
	}
	c := &Cache{
		source: source, queryTimeout: queryTimeout, refreshGate: make(chan struct{}, 1),
		flights:   make(map[lookupKey]*lookupFlight),
		observers: make(map[uint64]func()),
	}
	if err := c.Refresh(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// Run serially refreshes the complete snapshot every 60 seconds until ctx is
// canceled. It blocks until any current source call actually returns and then
// returns ctx.Err. Refresh failures are logged without source error text and
// do not erase the prior snapshot. Run may be called only once per Cache; the
// caller owns its goroutine and completion signal. It does not gate RPC or
// queue readiness and does not stop concurrent request lookups.
func (c *Cache) Run(ctx context.Context) error {
	if !c.runStarted.CompareAndSwap(false, true) {
		return errors.New("executor group cache refresh loop has already started")
	}
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()
	return c.run(ctx, ticker.C)
}

func (c *Cache) run(ctx context.Context, ticks <-chan time.Time) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticks:
			if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
				logx.Errorw("executor group cache refresh failed", logx.Field("error", err))
			}
		}
	}
}

// Refresh waits for other complete refreshes, loads all active groups, and
// replaces both indexes together only after a valid complete result. Waiting
// respects ctx; the database call additionally uses queryTimeout. After
// replacement, it synchronously invokes registered observers without holding
// the cache lock. Observers must return before the next complete refresh starts.
// Old in-flight misses may finish, but cannot publish a value read before this
// replacement. Failed loads do not invoke observers.
func (c *Cache) Refresh(ctx context.Context) error {
	select {
	case c.refreshGate <- struct{}{}:
		defer func() { <-c.refreshGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	queryCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()
	groups, err := c.source.LoadExecutionGroups(queryCtx)
	if err != nil {
		return sourceError("load executor group snapshot", err)
	}
	if err = queryCtx.Err(); err != nil {
		return err
	}
	byID := make(map[int64]repository.ExecutorGroupConfig, len(groups))
	byName := make(map[string]repository.ExecutorGroupConfig, len(groups))
	keys := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		if err = validate(group); err != nil {
			return err
		}
		_, duplicateID := byID[group.ID]
		_, duplicateName := byName[group.Name]
		_, duplicateKey := keys[group.DiscoveryKey]
		if duplicateID || duplicateName || duplicateKey {
			return errors.New("executor group snapshot contains duplicate identities")
		}
		byID[group.ID], byName[group.Name] = group, group
		keys[group.DiscoveryKey] = struct{}{}
	}
	c.mu.Lock()
	if err = ctx.Err(); err != nil {
		c.mu.Unlock()
		return err
	}
	// A miss started under an older snapshot must reread after replacement.
	// Otherwise a delayed query could restore a group removed by this load.
	c.generation++
	c.byID, c.byName = byID, byName
	observers := make([]func(), 0, len(c.observers))
	for _, observer := range c.observers {
		observers = append(observers, observer)
	}
	c.mu.Unlock()
	for _, observer := range observers {
		observer()
	}
	return nil
}

// Contains reports whether the current cache contains exactly group, without
// querying the source. It includes values loaded by a miss since the last full
// refresh. The result is an observation, not a lease: a refresh can remove the
// group immediately afterward. Resource owners must serialize installation and
// their OnRefresh cleanup to prevent late creation from undoing observed removal.
func (c *Cache) Contains(group repository.ExecutorGroupConfig) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	current, found := c.byID[group.ID]
	return found && current == group
}

// OnRefresh registers an observer for future successful complete replacements.
// It does not invoke observer initially or for individual miss loads. observer
// runs outside the cache lock and may read the cache, but must not panic or call
// Refresh synchronously. Cache changes may occur while it runs, so it must use
// current values rather than retain an earlier snapshot. A nil observer is a
// no-op. The returned function unregisters it; a callback already captured by a
// refresh may still run. The observer owns any additional synchronization.
func (c *Cache) OnRefresh(observer func()) func() {
	if observer == nil {
		return func() {}
	}
	c.mu.Lock()
	c.nextObserver++
	id := c.nextObserver
	c.observers[id] = observer
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.observers, id)
		c.mu.Unlock()
	}
}

// ByID returns a copy of an active group's execution configuration. A hit does
// not query the database; a miss uses the source and caches a valid result under
// both ID and name. Equal ID misses share one lookup. The initiating caller's
// cancellation cancels that shared lookup; other callers may retry a technical
// failure. Followers' cancellation stops only their own wait. ID and name are
// separate lookup keys until the configuration is known.
func (c *Cache) ByID(ctx context.Context, id int64) (repository.ExecutorGroupConfig, error) {
	if id <= 0 {
		return repository.ExecutorGroupConfig{}, errors.New("executor group ID must be positive")
	}
	return c.lookup(ctx, lookupKey{id: id})
}

// ByName is ByID's exact, case-sensitive name lookup. It does not trim names,
// cache negative results, or convert a source failure into ErrNotFound.
func (c *Cache) ByName(ctx context.Context, name string) (repository.ExecutorGroupConfig, error) {
	return c.lookup(ctx, lookupKey{name: name})
}

func (c *Cache) cached(key lookupKey) (repository.ExecutorGroupConfig, bool) {
	if key.id != 0 {
		config, ok := c.byID[key.id]
		return config, ok
	}
	config, ok := c.byName[key.name]
	return config, ok
}

func (c *Cache) lookup(ctx context.Context, key lookupKey) (repository.ExecutorGroupConfig, error) {
	if err := ctx.Err(); err != nil {
		return repository.ExecutorGroupConfig{}, err
	}
	c.mu.RLock()
	config, found := c.cached(key)
	c.mu.RUnlock()
	if found {
		return config, nil
	}
	c.mu.Lock()
	if config, ok := c.cached(key); ok {
		c.mu.Unlock()
		return config, nil
	}
	if flight, ok := c.flights[key]; ok {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return repository.ExecutorGroupConfig{}, ctx.Err()
		case <-flight.done:
			return flight.config, flight.err
		}
	}
	flight := &lookupFlight{done: make(chan struct{})}
	c.flights[key] = flight
	c.mu.Unlock()

	config, err := c.loadMiss(ctx, key)
	c.mu.Lock()
	flight.config, flight.err = config, err
	delete(c.flights, key)
	close(flight.done)
	c.mu.Unlock()
	return config, err
}

func (c *Cache) loadMiss(ctx context.Context, key lookupKey) (repository.ExecutorGroupConfig, error) {
	for {
		if err := ctx.Err(); err != nil {
			return repository.ExecutorGroupConfig{}, err
		}
		c.mu.RLock()
		config, found := c.cached(key)
		generation := c.generation
		c.mu.RUnlock()
		if found {
			return config, nil
		}
		loaded, err := c.query(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return repository.ExecutorGroupConfig{}, err
		}
		c.mu.Lock()
		if generation != c.generation {
			c.mu.Unlock()
			// Even a successful old lookup must be discarded. An absent entry in
			// the new full snapshot is not enough to classify current absence:
			// the group might instead have been created just after that snapshot.
			continue
		}
		if err == nil {
			c.byID[loaded.ID], c.byName[loaded.Name] = loaded, loaded
		}
		c.mu.Unlock()
		return loaded, err
	}
}

func (c *Cache) query(ctx context.Context, key lookupKey) (repository.ExecutorGroupConfig, error) {
	queryCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()
	var config *repository.ExecutorGroupConfig
	var err error
	if key.id != 0 {
		config, err = c.source.FindExecutionGroupByID(queryCtx, key.id)
	} else {
		config, err = c.source.FindExecutionGroupByName(queryCtx, key.name)
	}
	if contextErr := queryCtx.Err(); contextErr != nil {
		return repository.ExecutorGroupConfig{}, contextErr
	}
	if errors.Is(err, repository.ErrExecutorGroupUnavailable) {
		return repository.ExecutorGroupConfig{}, ErrNotFound
	}
	if err != nil {
		return repository.ExecutorGroupConfig{}, sourceError("load executor group", err)
	}
	if config == nil {
		return repository.ExecutorGroupConfig{}, errors.New("executor group source returned no configuration")
	}
	if err = validate(*config); err != nil {
		return repository.ExecutorGroupConfig{}, err
	}
	if (key.id != 0 && key.id != config.ID) || (key.id == 0 && key.name != config.Name) {
		return repository.ExecutorGroupConfig{}, errors.New("executor group source returned a different identity")
	}
	return *config, nil
}

func validate(config repository.ExecutorGroupConfig) error {
	if config.ID <= 0 {
		return errors.New("stored executor group ID is invalid")
	}
	if err := jobexecutor.ValidateCredentials(config.Name, config.Token); err != nil {
		return errors.New("stored executor group credentials are invalid")
	}
	key := config.DiscoveryKey
	if key == "" || key != strings.TrimSpace(key) || !utf8.ValidString(key) ||
		strings.ContainsRune(key, 0) || utf8.RuneCountInString(key) > 255 {
		return errors.New("stored executor group discovery key is invalid")
	}
	return nil
}

// lookupError keeps dependency identity available to errors.Is/As without
// including driver diagnostics in logs. Such diagnostics can contain SQL
// parameters or credential values and must not be formatted at this boundary.
type lookupError struct {
	operation string
	cause     error
}

func sourceError(operation string, cause error) error {
	return &lookupError{operation: operation, cause: cause}
}

func (e *lookupError) Error() string { return e.operation + " failed" }
func (e *lookupError) Unwrap() error { return e.cause }
