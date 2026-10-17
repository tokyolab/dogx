package logic

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/apps/job/internal/subcode"
	"github.com/tokyolab/dogx/apps/job/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/job/rpc/types/job"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validStoredDefinition(t *testing.T) *model.JobDefinition {
	t.Helper()
	definition, err := normalizeInput(completeInput(), false, testNow())
	if err != nil {
		t.Fatal(err)
	}
	definition.ID = 7
	return definition
}

func TestManagementTransactionsAndNotifications(t *testing.T) {
	r := &jobRepositoryStub{definition: validStoredDefinition(t), available: true}
	s := managementContext(r)
	notifications := 0
	s.NotifyJobChanged = func(ctx context.Context) error {
		notifications++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Error("notification lacks one-second budget")
		}
		return errors.New("etcd unavailable")
	}
	in := completeInput()
	in.Name = "  中文  "
	created, err := NewCreateJobLogic(context.Background(), s).CreateJob(&job.CreateJobRequest{Config: in})
	if err != nil || created.Id != 7 || r.saved.Name != "中文" || r.saved.NextTriggerAt != nil || notifications != 1 {
		t.Fatalf("created=%v saved=%+v notifications=%d err=%v", created, r.saved, notifications, err)
	}
	if strings.Join(r.calls, ",") != "group_lock,create" {
		t.Fatalf("creation lock order=%v", r.calls)
	}
	r.calls = nil
	if _, err := NewUpdateJobLogic(context.Background(), s).UpdateJob(&job.UpdateJobRequest{Id: 7, Config: in}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(r.calls, ",") != "group_lock,update" || r.saved.MaxRetries != 0 || r.saved.PostEnabled {
		t.Fatalf("update calls=%v saved=%+v", r.calls, r.saved)
	}
	r.calls = nil
	if _, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(1))}); err != nil || r.enabled == nil || !r.enabled.After(testNow()) {
		t.Fatalf("enable=%v err=%v", r.enabled, err)
	}
	if strings.Join(r.calls, ",") != "lock,available,enable" {
		t.Fatalf("enable lock order=%v", r.calls)
	}
	r.enabled, r.calls = nil, nil
	r.definition.Status = basemodel.RecordStatusEnabled
	if _, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(1))}); err != nil || r.enabled != nil || strings.Join(r.calls, ",") != "lock" {
		t.Fatalf("idempotent enable calls=%v err=%v", r.calls, err)
	}
	r.calls = nil
	if _, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(0))}); err != nil || strings.Join(r.calls, ",") != "disable" {
		t.Fatalf("disable calls=%v err=%v", r.calls, err)
	}
}

func TestEnabledCreateUsesTimeAfterGroupLock(t *testing.T) {
	r := &jobRepositoryStub{available: true}
	s := managementContext(r)
	s.Clock = func() time.Time {
		if len(r.calls) == 0 {
			return testNow()
		}
		return testNow().Add(2 * time.Minute)
	}
	_, err := NewCreateJobLogic(context.Background(), s).CreateJob(&job.CreateJobRequest{Config: validInput(), Status: pointer(int32(1))})
	if err != nil || r.saved.NextTriggerAt == nil || !r.saved.NextTriggerAt.After(testNow().Add(2*time.Minute)) {
		t.Fatalf("saved=%+v error=%v", r.saved, err)
	}
}

func TestDeleteRequiresDisabledAndNoActiveInstances(t *testing.T) {
	r := &jobRepositoryStub{definition: validStoredDefinition(t)}
	s := managementContext(r)
	r.definition.Status = basemodel.RecordStatusEnabled
	_, err := NewDeleteJobLogic(context.Background(), s).DeleteJob(&job.DeleteJobRequest{Id: 7})
	requireSubcode(t, err, subcode.JobStateNotAllowed)
	if strings.Join(r.calls, ",") != "lock" {
		t.Fatalf("enabled delete calls=%v", r.calls)
	}
	r.definition.Status, r.active, r.calls = basemodel.RecordStatusDisabled, true, nil
	_, err = NewDeleteJobLogic(context.Background(), s).DeleteJob(&job.DeleteJobRequest{Id: 7})
	requireSubcode(t, err, subcode.JobActiveInstances)
	if strings.Join(r.calls, ",") != "lock,active" {
		t.Fatalf("active delete calls=%v", r.calls)
	}
	r.active, r.calls = false, nil
	if _, err := NewDeleteJobLogic(context.Background(), s).DeleteJob(&job.DeleteJobRequest{Id: 7}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(r.calls, ",") != "lock,active,delete" {
		t.Fatalf("delete lock order=%v", r.calls)
	}
}

func TestManagementFailuresDoNotNotify(t *testing.T) {
	for _, test := range []struct {
		name  string
		steps []string
		call  func(*svc.ServiceContext) error
	}{
		{"create", []string{"group_lock", "create", "commit"}, func(s *svc.ServiceContext) error {
			_, err := NewCreateJobLogic(context.Background(), s).CreateJob(&job.CreateJobRequest{Config: validInput()})
			return err
		}},
		{"update", []string{"group_lock", "update", "commit"}, func(s *svc.ServiceContext) error {
			_, err := NewUpdateJobLogic(context.Background(), s).UpdateJob(&job.UpdateJobRequest{Id: 7, Config: completeInput()})
			return err
		}},
		{"enable", []string{"lock", "available", "enable", "commit"}, func(s *svc.ServiceContext) error {
			_, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(1))})
			return err
		}},
		{"disable", []string{"disable"}, func(s *svc.ServiceContext) error {
			_, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(0))})
			return err
		}},
		{"delete", []string{"lock", "active", "delete", "commit"}, func(s *svc.ServiceContext) error {
			_, err := NewDeleteJobLogic(context.Background(), s).DeleteJob(&job.DeleteJobRequest{Id: 7})
			return err
		}},
		{"get", []string{"get"}, func(s *svc.ServiceContext) error {
			_, err := NewGetJobLogic(context.Background(), s).GetJob(&job.GetJobRequest{Id: 7})
			return err
		}},
		{"list", []string{"list"}, func(s *svc.ServiceContext) error {
			_, err := NewListJobsLogic(context.Background(), s).ListJobs(&job.ListJobsRequest{Page: 1, PageSize: 20})
			return err
		}},
	} {
		for _, step := range test.steps {
			t.Run(test.name+"/"+step, func(t *testing.T) {
				failure := errors.New("unconfirmed database operation")
				r := &jobRepositoryStub{definition: validStoredDefinition(t), available: true, failures: map[string]error{step: failure}}
				s := managementContext(r)
				if step == "commit" {
					s.Transactions = func(_ context.Context, fn func(repository.Repositories) error) error {
						if err := fn(repository.Repositories{Jobs: r, Groups: r, Instances: r}); err != nil {
							return err
						}
						return failure
					}
				}
				s.NotifyJobChanged = func(context.Context) error { t.Error("unconfirmed operation notified"); return nil }
				if err := test.call(s); !errors.Is(err, failure) {
					t.Fatalf("error chain lost: %v", err)
				}
			})
		}
	}
}

func TestEnableRejectsInvalidStoredConfiguration(t *testing.T) {
	for _, change := range []func(*model.JobDefinition){
		func(j *model.JobDefinition) { j.Name = "\x00" }, func(j *model.JobDefinition) { j.Name = string([]byte{0xff}) },
		func(j *model.JobDefinition) { j.Name = strings.Repeat("界", 129) }, func(j *model.JobDefinition) { j.ProcessorName = " " },
		func(j *model.JobDefinition) { j.PostEnabled = true }, func(j *model.JobDefinition) { j.MaxRetries = -1 },
		func(j *model.JobDefinition) { j.TimeoutSeconds = 0 }, func(j *model.JobDefinition) { j.RetryIntervalSeconds = 0 },
		func(j *model.JobDefinition) { j.ExecutorGroupID = 0 }, func(j *model.JobDefinition) { j.ExecutionMode = 3 },
		func(j *model.JobDefinition) { j.BlockPolicy = 3 }, func(j *model.JobDefinition) { j.MisfirePolicy = 3 },
		func(j *model.JobDefinition) { j.CronExpr = "broken" },
	} {
		r := &jobRepositoryStub{definition: validStoredDefinition(t), available: true}
		change(r.definition)
		_, err := NewUpdateJobStatusLogic(context.Background(), managementContext(r)).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(1))})
		if status.Code(err) != codes.InvalidArgument || r.enabled != nil {
			t.Fatalf("accepted stored config=%+v err=%v", r.definition, err)
		}
	}
	if err := validateStoredDefinition(nil); status.Code(err) != codes.Internal {
		t.Fatalf("nil config error=%v", err)
	}
}

func TestEnableAndDeleteNilRecordAreInternal(t *testing.T) {
	s := managementContext(&jobRepositoryStub{available: true})
	_, err := NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(1))})
	if status.Code(err) != codes.Internal {
		t.Fatalf("enable nil record=%v", err)
	}
	_, err = NewDeleteJobLogic(context.Background(), s).DeleteJob(&job.DeleteJobRequest{Id: 7})
	if status.Code(err) != codes.Internal {
		t.Fatalf("delete nil record=%v", err)
	}
}

func TestUnavailableGroupBusinessError(t *testing.T) {
	r := &jobRepositoryStub{definition: validStoredDefinition(t)}
	s := managementContext(r)
	_, err := NewCreateJobLogic(context.Background(), s).CreateJob(&job.CreateJobRequest{Config: validInput()})
	requireSubcode(t, err, subcode.JobExecutorGroupUnavailable)
	_, err = NewUpdateJobStatusLogic(context.Background(), s).UpdateJobStatus(&job.UpdateJobStatusRequest{Id: 7, Status: pointer(int32(1))})
	requireSubcode(t, err, subcode.JobExecutorGroupUnavailable)
}

func TestNotificationRespectsUpstreamDeadlineAndCancellation(t *testing.T) {
	s := managementContext(&jobRepositoryStub{available: true})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()
	calls := 0
	s.NotifyJobChanged = func(ctx context.Context) error {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || !deadline.Equal(wantDeadline) {
			t.Errorf("notification deadline=%v want=%v", deadline, wantDeadline)
		}
		return errors.New("notification failed after confirmed commit")
	}
	if _, err := NewCreateJobLogic(ctx, s).CreateJob(&job.CreateJobRequest{Config: validInput()}); err != nil {
		t.Fatalf("notification changed save result: %v", err)
	}
	cancel()
	notifyJobChanged(ctx, s.NotifyJobChanged)
	if calls != 1 {
		t.Fatalf("canceled request still notified: calls=%d", calls)
	}
}
