package service

import (
	"errors"

	"github.com/tokyolab/dogx/apps/job/internal/repository"
)

var (
	ErrInvalidInput             = errors.New("invalid job input")
	ErrNotFound                 = repository.ErrJobNotFound
	ErrStateNotAllowed          = repository.ErrJobStateNotAllowed
	ErrExecutorGroupUnavailable = repository.ErrExecutorGroupUnavailable
	ErrActiveInstances          = errors.New("job has active instances")
	ErrBlocked                  = errors.New("job trigger blocked by an active instance")
	ErrRequestConflict          = errors.New("request identity already used with different parameters")
	ErrCapacityBusy             = errors.New("instance creation capacity is busy")
	ErrIndeterminate            = errors.New("trigger identity conflict could not be resolved")
)

const (
	SubcodeNotFound                 = "job.definition.not_found"
	SubcodeStateNotAllowed          = "job.definition.state_not_allowed"
	SubcodeActiveInstances          = "job.definition.active_instances"
	SubcodeExecutorGroupUnavailable = "job.definition.executor_group_unavailable"
	SubcodeBlocked                  = "job.trigger.blocked"
	SubcodeRequestConflict          = "job.trigger.request_conflict"
	SubcodeCapacityBusy             = "job.trigger.capacity_busy"
)
