// Package taskresult defines credential-free Task results and their persistence
// contracts independently of the execution RPC transport.
package taskresult

import "encoding/json"

// Outcome is the business conclusion of an execution attempt, independent of
// transport delivery, database persistence and the parent Instance outcome.
type Outcome uint8

const (
	OutcomeSuccess Outcome = iota + 1
	OutcomeFailure
)

// Report describes one attempt's result. GroupID must come from successful
// authentication, never from an untrusted request field. The remaining IDs and
// ExecutorInstanceID still require verification against persisted ownership,
// attempt and process-binding facts; authentication does not establish them.
// ExecutorInstanceID is the canonical UUIDv4 of the reporting process.
//
// Data and PreparedItems are mutually exclusive. Nil means absent; JSON "null"
// is present Data, and a non-nil empty PreparedItems means successful preparation
// of zero shards. Each prepared element defines one complete shard Task; a
// successful collection must not exceed jobexecutor.MaxPreparedItems.
// OutcomeFailure ignores both outputs, including malformed or oversized data.
// Summary is the business result description, not a transport or database error.
//
// Report owns Data and every PreparedItems byte slice. Boundary adapters must
// copy borrowed buffers before handing it to a consumer, which may retain it
// beyond the request lifetime. Once handed over, producers must not mutate that
// storage. Consumers treat it as immutable and copy it before making changes.
type Report struct {
	GroupID            int64
	JobID              int64
	InstanceID         int64
	TaskID             int64
	AttemptNo          int64
	ExecutorInstanceID string
	Outcome            Outcome
	Summary            string
	Data               json.RawMessage
	PreparedItems      []json.RawMessage
}
