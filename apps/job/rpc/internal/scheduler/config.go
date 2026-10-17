package scheduler

import (
	"errors"
	"path"
	"strings"
	"time"
)

// Config defines process-local trigger resources, not Job execution timeouts or
// retries. New defaults zero numeric values and rejects negative values. Prefix
// must be supplied explicitly.
type Config struct {
	// Prefix must distinguish both the environment and the logical cluster.
	Prefix string
	// QueueCapacity bounds queued and in-flight distinct Job IDs, not Tasks per Job.
	QueueCapacity int
	// CreationConcurrency counts automatic workers; automatic and manual creation
	// still share the Creator's permits.
	CreationConcurrency int
	// LeaseTTLSeconds is the election lease TTL in seconds, not an execution deadline.
	LeaseTTLSeconds int
	// RecoveryLimit bounds candidates per recovery pass, not total backlog recovery.
	RecoveryLimit int
	// QueryTimeout bounds rule and recovery queries, not Instance creation transactions.
	QueryTimeout time.Duration
}

func (c Config) defaults() (Config, error) {
	if c.Prefix == "" || c.Prefix == "/" || !strings.HasPrefix(c.Prefix, "/") || path.Clean(c.Prefix) != c.Prefix || strings.ContainsAny(c.Prefix, "\\\x00\r\n") {
		return c, errors.New("scheduler prefix must be an absolute clean environment/cluster namespace")
	}
	if c.QueueCapacity < 0 || c.CreationConcurrency < 0 || c.LeaseTTLSeconds < 0 || c.RecoveryLimit < 0 || c.QueryTimeout < 0 {
		return c, errors.New("scheduler capacity, concurrency, and lease TTL must be positive")
	}
	if c.QueueCapacity == 0 {
		c.QueueCapacity = 1024
	}
	if c.CreationConcurrency == 0 {
		c.CreationConcurrency = 32
	}
	if c.LeaseTTLSeconds == 0 {
		c.LeaseTTLSeconds = 15
	}
	if c.RecoveryLimit == 0 {
		c.RecoveryLimit = 1024
	}
	if c.QueryTimeout == 0 {
		c.QueryTimeout = 5 * time.Second
	}
	return c, nil
}

type keys struct{ election, changed string }

func makeKeys(prefix string) keys {
	// Election itself adds its trailing slash. Keep the two namespaces here;
	// callers never construct lease-bound configuration notification keys.
	return keys{prefix + "/scheduler/election", prefix + "/scheduler/job-config-changed"}
}

type timings struct {
	query, request, notify time.Duration
	config, recovery       time.Duration
	backoffMin, backoffMax time.Duration
}

func defaultTimings() timings {
	return timings{query: 5 * time.Second, request: 5 * time.Second, notify: time.Second,
		config: time.Minute, recovery: time.Minute, backoffMin: time.Second, backoffMax: 30 * time.Second}
}
