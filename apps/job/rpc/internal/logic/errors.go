package logic

import (
	"context"
	"errors"

	"github.com/tokyolab/dogx/apps/job/internal/service"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func serviceError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, service.ErrInvalidInput) {
		return invalidInput()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "job request timed out")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "job request canceled")
	}
	for _, entry := range []struct {
		err              error
		subcode, message string
	}{
		{service.ErrNotFound, service.SubcodeNotFound, "Job does not exist"},
		{service.ErrStateNotAllowed, service.SubcodeStateNotAllowed, "Disable the Job before changing it"},
		{service.ErrActiveInstances, service.SubcodeActiveInstances, "Unfinished instances prevent deletion"},
		{service.ErrExecutorGroupUnavailable, service.SubcodeExecutorGroupUnavailable, "Executor group is unavailable"},
		{service.ErrBlocked, service.SubcodeBlocked, "An unfinished instance blocks this trigger"},
		{service.ErrRequestConflict, service.SubcodeRequestConflict, "The request ID has different parameters"},
		{service.ErrCapacityBusy, service.SubcodeCapacityBusy, "Instance creation capacity is busy"},
	} {
		if errors.Is(err, entry.err) {
			return bizerror.New(entry.subcode, entry.message)
		}
	}
	return err
}
