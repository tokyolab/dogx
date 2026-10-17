package logic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/internal/trigger"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func requireSubcode(t *testing.T, err error, want string) {
	t.Helper()
	business, ok := bizerror.From(err)
	if !ok || business.Code() != bizerror.DefaultCode || business.Subcode() != want {
		t.Fatalf("business error=%v want=%s", err, want)
	}
}

func TestJobStatusRejectsValuesBeforeNarrowing(t *testing.T) {
	for _, value := range []int32{-1, 2, 65536, 65537, -65536, -65535, math.MinInt32, math.MaxInt32} {
		for _, test := range []struct {
			name string
			call func(*svc.ServiceContext) error
		}{
			{"create", func(s *svc.ServiceContext) error {
				_, err := NewCreateJobLogic(context.Background(), s).CreateJob(&job.CreateJobRequest{Config: validInput(), Status: &value})
				return err
			}},
			{"update status", func(s *svc.ServiceContext) error {
				_, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: &value})
				return err
			}},
			{"list", func(s *svc.ServiceContext) error {
				_, err := NewListJobsLogic(context.Background(), s).ListJobs(&job.ListJobsRequest{Page: 1, PageSize: 20, Status: &value})
				return err
			}},
		} {
			t.Run(fmt.Sprintf("%s/%d", test.name, value), func(t *testing.T) {
				r := &jobRepositoryStub{}
				err := test.call(managementContext(r))
				if status.Code(err) != codes.InvalidArgument || len(r.calls) != 0 {
					t.Fatalf("calls=%v error=%v", r.calls, err)
				}
			})
		}
	}
}

func TestJobOperationsRejectInvalidRequestsBeforeDependencies(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*svc.ServiceContext) error
	}{
		{"create nil", func(s *svc.ServiceContext) error {
			_, err := NewCreateJobLogic(context.Background(), s).CreateJob(nil)
			return err
		}},
		{"create config", func(s *svc.ServiceContext) error {
			_, err := NewCreateJobLogic(context.Background(), s).CreateJob(&job.CreateJobRequest{})
			return err
		}},
		{"update nil", func(s *svc.ServiceContext) error {
			_, err := NewUpdateJobLogic(context.Background(), s).UpdateJob(nil)
			return err
		}},
		{"update id", func(s *svc.ServiceContext) error {
			_, err := NewUpdateJobLogic(context.Background(), s).UpdateJob(&job.UpdateJobRequest{Config: completeInput()})
			return err
		}},
		{"update config", func(s *svc.ServiceContext) error {
			_, err := NewUpdateJobLogic(context.Background(), s).UpdateJob(&job.UpdateJobRequest{Id: 7, Config: validInput()})
			return err
		}},
		{"status nil", func(s *svc.ServiceContext) error {
			_, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(nil)
			return err
		}},
		{"status missing", func(s *svc.ServiceContext) error {
			_, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7})
			return err
		}},
		{"delete nil", func(s *svc.ServiceContext) error {
			_, err := NewDeleteJobLogic(context.Background(), s).DeleteJob(nil)
			return err
		}},
		{"delete id", func(s *svc.ServiceContext) error {
			_, err := NewDeleteJobLogic(context.Background(), s).DeleteJob(&job.DeleteJobRequest{})
			return err
		}},
		{"get nil", func(s *svc.ServiceContext) error {
			_, err := NewGetJobLogic(context.Background(), s).GetJob(nil)
			return err
		}},
		{"get id", func(s *svc.ServiceContext) error {
			_, err := NewGetJobLogic(context.Background(), s).GetJob(&job.GetJobRequest{})
			return err
		}},
		{"list nil", func(s *svc.ServiceContext) error {
			_, err := NewListJobsLogic(context.Background(), s).ListJobs(nil)
			return err
		}},
		{"trigger nil", func(s *svc.ServiceContext) error {
			_, err := NewTriggerJobLogic(context.Background(), s).TriggerJob(nil)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Nil dependencies make an unexpected dependency call fail immediately.
			if err := test.call(&svc.ServiceContext{Clock: testNow}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestJobProjectionAndCronPreview(t *testing.T) {
	now := testNow()
	r := &jobRepositoryStub{items: []repository.JobListItem{{ID: 7, Status: basemodel.RecordStatusDisabled, NextTriggerAt: &now, CreatedAt: now, UpdatedAt: now}, {ID: 8, Status: basemodel.RecordStatusEnabled, NextTriggerAt: &now}}, total: 2}
	s := managementContext(r)
	list, err := NewListJobsLogic(context.Background(), s).ListJobs(&job.ListJobsRequest{Page: 1, PageSize: 20})
	if err != nil || list.Total != 2 || len(list.Items) != 2 {
		t.Fatalf("list=%+v error=%v", list, err)
	}
	if list.Items[0].NextTriggerAt != nil || list.Items[0].ExecutorGroupName != nil || list.Items[0].CreatedAt != "2026-10-16T12:00:00.123456+08:00" || *list.Items[1].NextTriggerAt != now.Format(time.RFC3339Nano) {
		t.Fatalf("projection=%+v", list.Items)
	}
	r.items = nil
	list, err = NewListJobsLogic(context.Background(), s).ListJobs(&job.ListJobsRequest{Page: 1, PageSize: 20})
	if err != nil || list.Items == nil {
		t.Fatalf("empty list=%+v error=%v", list, err)
	}
	_, err = NewGetJobLogic(context.Background(), s).GetJob(&job.GetJobRequest{Id: 7})
	if status.Code(err) != codes.Internal {
		t.Fatalf("nil detail contract error=%v", err)
	}
	r.err = repository.ErrJobNotFound
	_, err = NewGetJobLogic(context.Background(), s).GetJob(&job.GetJobRequest{Id: 7})
	requireSubcode(t, err, subcode.JobNotFound)
	r.err = nil
	r.detail = &repository.JobDetail{JobDefinition: model.JobDefinition{Base: basemodel.Base{ID: 7}, Status: basemodel.RecordStatusEnabled, NextTriggerAt: &now, Remark: "note", Params: "raw", MaxRetries: 0, TimeoutSeconds: 900}, ExecutorGroupName: pointer("group")}
	detail, err := NewGetJobLogic(context.Background(), s).GetJob(&job.GetJobRequest{Id: 7})
	if err != nil || detail.Job.Remark != "note" || detail.Job.Params != "raw" || *detail.Job.ExecutorGroupName != "group" || *detail.Job.NextTriggerAt != now.Format(time.RFC3339Nano) {
		t.Fatalf("detail=%+v error=%v", detail, err)
	}
	reads := 0
	s.Clock = func() time.Time { reads++; return now }
	preview, err := NewPreviewJobCronLogic(context.Background(), s).PreviewJobCron(&job.PreviewJobCronRequest{CronExpr: " 0 * * * * * "})
	wantFirst := now.Truncate(time.Minute).Add(time.Minute).In(time.Local).Format(time.RFC3339Nano)
	wantLast := now.Truncate(time.Minute).Add(5 * time.Minute).In(time.Local).Format(time.RFC3339Nano)
	if err != nil || reads != 1 || len(preview.Times) != 5 || preview.Times[0] != wantFirst || preview.Times[4] != wantLast {
		t.Fatalf("preview=%+v reads=%d error=%v", preview, reads, err)
	}
	for _, request := range []*job.PreviewJobCronRequest{nil, {CronExpr: " "}, {CronExpr: "0 0 0 30 2 *"}} {
		if _, err := NewPreviewJobCronLogic(context.Background(), s).PreviewJobCron(request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("preview error=%v", err)
		}
	}
}

func TestManualTriggerBoundary(t *testing.T) {
	for _, test := range []struct {
		err     error
		subcode string
	}{
		{nil, ""}, {repository.ErrJobNotFound, subcode.JobNotFound},
		{repository.ErrExecutorGroupUnavailable, subcode.JobExecutorGroupUnavailable},
		{trigger.ErrBlocked, subcode.JobTriggerBlocked}, {trigger.ErrRequestConflict, subcode.JobTriggerRequestConflict}, {trigger.ErrCapacityBusy, subcode.JobTriggerCapacityBusy},
	} {
		t.Run(test.subcode, func(t *testing.T) {
			stub := &manualTriggerStub{err: test.err}
			if test.err != nil {
				stub.err = fmt.Errorf("trigger transaction: %w", test.err)
			}
			result, err := NewTriggerJobLogic(context.Background(), &svc.ServiceContext{Trigger: stub}).TriggerJob(&job.TriggerJobRequest{Id: 7, RequestId: "12051a0f-76b8-4d88-897f-15ec600de323", OperatorId: 42, ParamsOverridden: pointer(false)})
			if !stub.called || stub.input.JobID != 7 || stub.input.OperatorID != 42 || stub.input.ParamsOverridden {
				t.Fatalf("trigger input=%+v", stub.input)
			}
			if test.err == nil {
				if err != nil || result.InstanceId != 9 {
					t.Fatalf("result=%+v error=%v", result, err)
				}
			} else {
				requireSubcode(t, err, test.subcode)
			}
		})
	}
	if err := manualTriggerError(trigger.ErrInvalidInput); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid request error=%v", err)
	}
	err := manualTriggerError(fmt.Errorf("request identity: %w", trigger.ErrIndeterminate))
	if !errors.Is(err, trigger.ErrIndeterminate) {
		t.Fatalf("technical cause lost: %v", err)
	}
	if business, ok := bizerror.From(err); ok {
		t.Fatalf("indeterminate converted to business rejection: %v", business)
	}
}

func TestRPCErrorClassificationPreservesTechnicalCauses(t *testing.T) {
	for _, convert := range []func(error) error{
		func(err error) error { return jobManagementError("update job", err) }, manualTriggerError,
		func(err error) error { return rpcOperationError("list jobs", err) },
	} {
		for _, test := range []struct {
			err  error
			code codes.Code
		}{{context.DeadlineExceeded, codes.DeadlineExceeded}, {context.Canceled, codes.Canceled}} {
			err := convert(fmt.Errorf("storage request: %w", test.err))
			if status.Code(err) != test.code || !errors.Is(err, test.err) {
				t.Fatalf("context error=%v code=%v", err, status.Code(err))
			}
		}
		technical := &testTechnicalError{}
		err := convert(fmt.Errorf("database unavailable: %w", technical))
		var found *testTechnicalError
		if !errors.Is(err, technical) || !errors.As(err, &found) {
			t.Fatalf("technical error chain lost: %v", err)
		}
		if err := convert(nil); err != nil {
			t.Fatalf("nil error=%v", err)
		}
	}
	for _, test := range []struct {
		err     error
		subcode string
	}{
		{repository.ErrJobNotFound, subcode.JobNotFound}, {repository.ErrJobStateNotAllowed, subcode.JobStateNotAllowed}, {repository.ErrExecutorGroupUnavailable, subcode.JobExecutorGroupUnavailable},
	} {
		requireSubcode(t, jobManagementError("manage job", fmt.Errorf("repository: %w", test.err)), test.subcode)
	}
}

type testTechnicalError struct{}

func (*testTechnicalError) Error() string { return "technical failure" }
