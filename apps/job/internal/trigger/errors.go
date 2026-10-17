package trigger

import "errors"

var (
	ErrInvalidInput    = errors.New("invalid trigger input")
	ErrBlocked         = errors.New("job trigger blocked by an active instance")
	ErrRequestConflict = errors.New("request identity already used with different parameters")
	ErrCapacityBusy    = errors.New("instance creation capacity is busy")
	// ErrIndeterminate is a technical failure, not a business rejection: a
	// conflicting identity could not be observed by the subsequent fresh read.
	ErrIndeterminate = errors.New("trigger identity conflict could not be resolved")
)
