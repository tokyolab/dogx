package trigger

import "errors"

var (
	// ErrInvalidInput rejects an invalid trigger ID, operator, or parameters
	// before entering a transaction.
	ErrInvalidInput = errors.New("invalid trigger input")
	// ErrBlocked rejects a new manual round under the discard policy because
	// another Instance has not finished.
	ErrBlocked = errors.New("job trigger blocked by an active instance")
	// ErrRequestConflict means a manual request identity for this Job was
	// already used with a different override flag or parameters.
	ErrRequestConflict = errors.New("request identity already used with different parameters")
	// ErrCapacityBusy means a manual trigger could not obtain a process-local
	// creation permit. The caller may retry using the same request identity.
	ErrCapacityBusy = errors.New("instance creation capacity is busy")
	// ErrIndeterminate means an insert conflicted but a fresh statement still
	// could not read the conflicting identity. This is a technical failure to
	// confirm the original Instance, not a business rejection or permission to
	// retry with a new request identity.
	ErrIndeterminate = errors.New("trigger identity conflict could not be resolved")
)
