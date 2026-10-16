package logic

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/service"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	"github.com/tokyolab/dogx/pkg/bizerror"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestJobOperationsSuccessAndServiceFailures(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*svc.ServiceContext) (any, error)
	}{
		{"CreateJob", func(s *svc.ServiceContext) (any, error) {
			return NewCreateJobLogic(context.Background(), s).CreateJob(&job.CreateJobRequest{Config: validInput(), Status: pointer(int32(1))})
		}},
		{"UpdateJob", func(s *svc.ServiceContext) (any, error) {
			return NewUpdateJobLogic(context.Background(), s).UpdateJob(&job.UpdateJobRequest{Id: 7, Config: completeInput()})
		}},
		{"UpdateJobStatus", func(s *svc.ServiceContext) (any, error) {
			return NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(0))})
		}},
		{"DeleteJob", func(s *svc.ServiceContext) (any, error) {
			return NewDeleteJobLogic(context.Background(), s).DeleteJob(&job.DeleteJobRequest{Id: 7})
		}},
		{"GetJob", func(s *svc.ServiceContext) (any, error) {
			return NewGetJobLogic(context.Background(), s).GetJob(&job.GetJobRequest{Id: 7})
		}},
		{"ListJobs", func(s *svc.ServiceContext) (any, error) {
			return NewListJobsLogic(context.Background(), s).ListJobs(&job.ListJobsRequest{Page: 1, PageSize: 20})
		}},
		{"TriggerManual", func(s *svc.ServiceContext) (any, error) {
			return NewTriggerJobLogic(context.Background(), s).TriggerJob(&job.TriggerJobRequest{Id: 7, RequestId: "12051a0f-76b8-4d88-897f-15ec600de323", OperatorId: 42, ParamsOverridden: pointer(false)})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			technical := errors.New("database host secret.internal failed")
			for _, failure := range []error{nil, service.ErrNotFound, technical} {
				stub := &jobServiceStub{detail: &repository.JobDetail{JobDefinition: model.JobDefinition{Base: basemodel.Base{ID: 7}}}, err: failure}
				result, err := test.call(&svc.ServiceContext{Jobs: stub, Clock: testNow})
				if stub.called != test.name {
					t.Fatalf("called=%s", stub.called)
				}
				switch {
				case failure == nil:
					if err != nil || result == nil {
						t.Fatalf("result=%+v error=%v", result, err)
					}
				case errors.Is(failure, service.ErrNotFound):
					business, ok := bizerror.From(err)
					if !ok || business.Subcode() != service.SubcodeNotFound {
						t.Fatalf("business error=%v", err)
					}
				default:
					if !errors.Is(err, technical) {
						t.Fatalf("technical error lost: %v", err)
					}
				}
				if test.name == "CreateJob" && (stub.definition.Status != model.StatusEnabled || stub.definition.Name != "job") {
					t.Fatalf("create input=%+v", stub.definition)
				}
				if test.name == "UpdateJob" && (stub.id != 7 || stub.definition.MaxRetries != 0) {
					t.Fatalf("update input=%+v", stub.definition)
				}
				if test.name == "UpdateJobStatus" && (stub.id != 7 || stub.status != model.StatusDisabled) {
					t.Fatalf("status input=%d %d", stub.id, stub.status)
				}
				if test.name == "TriggerManual" && (stub.manual.JobID != 7 || stub.manual.OperatorID != 42 || stub.manual.ParamsOverridden) {
					t.Fatalf("trigger input=%+v", stub.manual)
				}
			}
		})
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
		{"create status", func(s *svc.ServiceContext) error {
			_, err := NewCreateJobLogic(context.Background(), s).CreateJob(&job.CreateJobRequest{Config: validInput(), Status: pointer(int32(2))})
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
		{"status invalid", func(s *svc.ServiceContext) error {
			_, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(2))})
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
			stub := &jobServiceStub{}
			err := test.call(&svc.ServiceContext{Jobs: stub, Clock: testNow})
			if status.Code(err) != codes.InvalidArgument || stub.called != "" {
				t.Fatalf("called=%s error=%v", stub.called, err)
			}
		})
	}
}

func TestJobProjectionAndCronPreview(t *testing.T) {
	now := testNow()
	stub := &jobServiceStub{items: []repository.JobListItem{{ID: 7, Status: model.StatusDisabled, NextTriggerAt: &now, CreatedAt: now, UpdatedAt: now}, {ID: 8, Status: model.StatusEnabled, NextTriggerAt: &now}}, total: 2}
	s := &svc.ServiceContext{Jobs: stub, Clock: testNow}
	list, err := NewListJobsLogic(context.Background(), s).ListJobs(&job.ListJobsRequest{Page: 1, PageSize: 20})
	if err != nil || list.Total != 2 || len(list.Items) != 2 {
		t.Fatalf("list=%+v error=%v", list, err)
	}
	if list.Items[0].NextTriggerAt != nil || list.Items[0].ExecutorGroupName != nil || list.Items[0].CreatedAt != "2026-10-16T12:00:00.123456+08:00" || *list.Items[1].NextTriggerAt != now.Format(time.RFC3339Nano) {
		t.Fatalf("projection=%+v", list.Items)
	}
	stub.items = nil
	list, err = NewListJobsLogic(context.Background(), s).ListJobs(&job.ListJobsRequest{Page: 1, PageSize: 20})
	if err != nil || list.Items == nil {
		t.Fatalf("empty list=%+v error=%v", list, err)
	}
	_, err = NewGetJobLogic(context.Background(), s).GetJob(&job.GetJobRequest{Id: 7})
	if business, ok := bizerror.From(err); !ok || business.Subcode() != service.SubcodeNotFound {
		t.Fatalf("missing detail=%v", err)
	}
	stub.detail = &repository.JobDetail{JobDefinition: model.JobDefinition{Base: basemodel.Base{ID: 7}, Status: model.StatusEnabled, NextTriggerAt: &now, Remark: "note", Params: "raw", MaxRetries: 0, TimeoutSeconds: 900}, ExecutorGroupName: pointer("group")}
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

func TestServiceErrorClassification(t *testing.T) {
	for _, test := range []struct {
		err     error
		subcode string
	}{
		{service.ErrNotFound, service.SubcodeNotFound}, {service.ErrStateNotAllowed, service.SubcodeStateNotAllowed}, {service.ErrActiveInstances, service.SubcodeActiveInstances}, {service.ErrExecutorGroupUnavailable, service.SubcodeExecutorGroupUnavailable}, {service.ErrBlocked, service.SubcodeBlocked}, {service.ErrRequestConflict, service.SubcodeRequestConflict}, {service.ErrCapacityBusy, service.SubcodeCapacityBusy},
	} {
		t.Run(test.subcode, func(t *testing.T) {
			err := serviceError(fmt.Errorf("operation failed: %w", test.err))
			business, ok := bizerror.From(err)
			if !ok || business.Code() != bizerror.DefaultCode || business.Subcode() != test.subcode {
				t.Fatalf("error=%v", err)
			}
		})
	}
	for _, test := range []struct {
		err  error
		code codes.Code
	}{{nil, codes.OK}, {service.ErrInvalidInput, codes.InvalidArgument}, {context.DeadlineExceeded, codes.DeadlineExceeded}, {context.Canceled, codes.Canceled}} {
		if err := serviceError(test.err); status.Code(err) != test.code {
			t.Fatalf("error=%v want=%v", err, test.code)
		}
	}
}
