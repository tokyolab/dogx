package logic

import (
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/schedule"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func invalidInput() error { return status.Error(codes.InvalidArgument, "invalid job request") }
func normalizeText(value string, required bool, capacity int) (string, error) {
	// Check the decoded text before trimming: NUL and malformed UTF-8 never become valid input.
	if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return "", invalidInput()
	}
	value = strings.TrimSpace(value)
	if (required && value == "") || (capacity > 0 && utf8.RuneCountInString(value) > capacity) {
		return "", invalidInput()
	}
	return value, nil
}
func normalizeInput(in *job.JobInput, create bool, now time.Time) (*model.JobDefinition, error) {
	if in == nil || in.ExecutorGroupId <= 0 || in.TimeoutSeconds <= 0 {
		return nil, invalidInput()
	}
	if !create && (in.ExecutionMode == nil || in.PostEnabled == nil || in.MisfirePolicy == nil || in.BlockPolicy == nil || in.MaxRetries == nil || in.RetryIntervalSeconds == nil) {
		return nil, invalidInput()
	}
	execution, post, misfire, block, retries, interval := int32(1), false, int32(0), int32(0), int32(3), int32(10)
	if in.ExecutionMode != nil {
		execution = *in.ExecutionMode
	}
	if in.PostEnabled != nil {
		post = *in.PostEnabled
	}
	if in.MisfirePolicy != nil {
		misfire = *in.MisfirePolicy
	}
	if in.BlockPolicy != nil {
		block = *in.BlockPolicy
	}
	if in.MaxRetries != nil {
		retries = *in.MaxRetries
	}
	if in.RetryIntervalSeconds != nil {
		interval = *in.RetryIntervalSeconds
	}
	if (execution != 1 && execution != 2) || (execution == 1 && post) || !validStatus(misfire) || !validStatus(block) || retries < 0 || interval <= 0 {
		return nil, invalidInput()
	}
	// Keep duration conversions explicit even though current int32 seconds fit int64 nanoseconds.
	if int64(interval) > math.MaxInt64/int64(time.Second) || int64(in.TimeoutSeconds) > math.MaxInt64/int64(time.Second) {
		return nil, invalidInput()
	}
	result := &model.JobDefinition{ExecutorGroupID: in.ExecutorGroupId, ExecutionMode: model.ExecutionMode(execution), PostEnabled: post, MisfirePolicy: model.MisfirePolicy(misfire), BlockPolicy: model.BlockPolicy(block), MaxRetries: retries, RetryIntervalSeconds: interval, TimeoutSeconds: in.TimeoutSeconds}
	var err error
	if result.Name, err = normalizeText(in.Name, true, 128); err != nil {
		return nil, err
	}
	if result.Remark, err = normalizeText(in.Remark, false, 500); err != nil {
		return nil, err
	}
	if result.ProcessorName, err = normalizeText(in.ProcessorName, true, 128); err != nil {
		return nil, err
	}
	if result.Params, err = normalizeText(in.Params, false, 0); err != nil {
		return nil, err
	}
	if result.CronExpr, err = normalizeText(in.CronExpr, true, 128); err != nil {
		return nil, err
	}
	if _, err = schedule.Next(result.CronExpr, now); err != nil {
		return nil, invalidInput()
	}
	return result, nil
}
func validStatus(value int32) bool { return value == 0 || value == 1 }
func normalizeFilter(in *job.ListJobsRequest) (repository.JobFilter, error) {
	if in == nil || in.Page < 1 || in.PageSize < 1 || in.PageSize > 200 || in.Page-1 > math.MaxInt64/in.PageSize || (in.Id != nil && *in.Id <= 0) || (in.ExecutorGroupId != nil && *in.ExecutorGroupId <= 0) || (in.Status != nil && !validStatus(*in.Status)) {
		return repository.JobFilter{}, invalidInput()
	}
	name, err := normalizeText(in.Name, false, 128)
	if err != nil {
		return repository.JobFilter{}, err
	}
	result := repository.JobFilter{Page: in.Page, PageSize: in.PageSize, ID: in.Id, Name: name, ExecutorGroupID: in.ExecutorGroupId}
	if in.Status != nil {
		value := model.Status(*in.Status)
		result.Status = &value
	}
	return result, nil
}
func normalizeTrigger(in *job.TriggerJobRequest) (service.ManualTrigger, error) {
	if in == nil || in.Id <= 0 || in.OperatorId <= 0 || in.ParamsOverridden == nil {
		return service.ManualTrigger{}, invalidInput()
	}
	// Presence is checked before normalization: even an empty string is a supplied override.
	if (*in.ParamsOverridden && in.Params == nil) || (!*in.ParamsOverridden && in.Params != nil) {
		return service.ManualTrigger{}, invalidInput()
	}
	requestID, err := uuid.Parse(in.RequestId)
	if err != nil || len(in.RequestId) != 36 {
		return service.ManualTrigger{}, invalidInput()
	}
	result := service.ManualTrigger{JobID: in.Id, RequestID: requestID, ParamsOverridden: *in.ParamsOverridden, OperatorID: in.OperatorId}
	if in.Params != nil {
		result.Params, err = normalizeText(*in.Params, false, 0)
		if err != nil {
			return service.ManualTrigger{}, err
		}
	}
	return result, nil
}
