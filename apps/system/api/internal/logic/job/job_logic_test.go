package job

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/pkg/bizerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
)

func identityContext() context.Context {
	ctx := context.WithValue(context.Background(), "userId", int64(42))
	ctx = context.WithValue(ctx, "sessionId", "session")
	return context.WithValue(ctx, "isSuperAdmin", false)
}
func TestJobLogicTransfersPresenceAndIdentity(t *testing.T) {
	rpc := &jobRPCStub{}
	s := &svc.ServiceContext{JobRpc: rpc}
	ctx := identityContext()
	_, err := NewCreateJobLogic(ctx, s).CreateJob(&types.CreateJobReq{Name: " job ", ExecutorGroupId: 1, ProcessorName: "p", CronExpr: "0 * * * * *", TimeoutSeconds: 900})
	if err != nil {
		t.Fatal(err)
	}
	create := rpc.request.(*jobclient.CreateJobRequest)
	if create.Status != nil || create.Config.ExecutionMode != nil || create.Config.MaxRetries != nil || create.Config.Name != " job " {
		t.Fatalf("BFF changed defaults or strings: %+v", create)
	}
	zero := int32(0)
	post := false
	_, err = NewUpdateJobLogic(ctx, s).UpdateJob(&types.UpdateJobReq{Id: 1, ExecutionMode: 1, PostEnabled: &post, MisfirePolicy: &zero, BlockPolicy: &zero, MaxRetries: &zero, RetryIntervalSeconds: 10, TimeoutSeconds: 900})
	if err != nil {
		t.Fatal(err)
	}
	update := rpc.request.(*jobclient.UpdateJobRequest)
	if update.Config.PostEnabled == nil || *update.Config.PostEnabled || update.Config.MaxRetries == nil || *update.Config.MaxRetries != 0 || update.Config.ExecutionMode == nil || *update.Config.ExecutionMode != 1 {
		t.Fatalf("lost explicit values: %+v", update)
	}
	params := "  "
	_, err = NewTriggerJobLogic(ctx, s).TriggerJob(&types.TriggerJobReq{Id: 1, ParamsOverridden: &post, Params: &params})
	if err != nil {
		t.Fatal(err)
	}
	trigger := rpc.request.(*jobclient.TriggerJobRequest)
	if trigger.OperatorId != 42 || trigger.Params != &params || trigger.ParamsOverridden != &post {
		t.Fatalf("lost identity/presence: %+v", trigger)
	}
	if _, err = NewTriggerJobLogic(context.Background(), s).TriggerJob(&types.TriggerJobReq{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("identity error=%v", err)
	}
}
func TestJobLogicOperationsAndErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*svc.ServiceContext) (any, error)
	}{
		{"CreateJob", func(s *svc.ServiceContext) (any, error) {
			return NewCreateJobLogic(identityContext(), s).CreateJob(&types.CreateJobReq{})
		}},
		{"UpdateJob", func(s *svc.ServiceContext) (any, error) {
			return NewUpdateJobLogic(identityContext(), s).UpdateJob(&types.UpdateJobReq{})
		}},
		{"UpdateJobStatus", func(s *svc.ServiceContext) (any, error) {
			return NewUpdateJobStatusLogic(identityContext(), s).UpdateJobStatus(&types.UpdateJobStatusReq{})
		}},
		{"DeleteJob", func(s *svc.ServiceContext) (any, error) {
			return NewDeleteJobLogic(identityContext(), s).DeleteJob(&types.IDReq{})
		}},
		{"GetJob", func(s *svc.ServiceContext) (any, error) {
			return NewGetJobLogic(identityContext(), s).GetJob(&types.IDReq{})
		}},
		{"ListJobs", func(s *svc.ServiceContext) (any, error) {
			return NewListJobsLogic(identityContext(), s).ListJobs(&types.JobListReq{})
		}},
		{"PreviewJobCron", func(s *svc.ServiceContext) (any, error) {
			return NewPreviewJobCronLogic(identityContext(), s).PreviewJobCron(&types.PreviewJobCronReq{})
		}},
		{"TriggerJob", func(s *svc.ServiceContext) (any, error) {
			return NewTriggerJobLogic(identityContext(), s).TriggerJob(&types.TriggerJobReq{})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, failure := range []error{nil, bizerror.New("job.trigger.blocked", "blocked"), status.Error(codes.Unavailable, "internal host")} {
				rpc := &jobRPCStub{err: failure}
				_, err := test.call(&svc.ServiceContext{JobRpc: rpc})
				if !errors.Is(err, failure) || rpc.called != test.name {
					t.Fatalf("call=%s err=%v want=%v", rpc.called, err, failure)
				}
			}
			rpc := &jobRPCStub{invalid: true}
			if _, err := test.call(&svc.ServiceContext{JobRpc: rpc}); status.Code(err) != codes.Internal {
				t.Fatalf("nil RPC response accepted: %v", err)
			}
		})
	}
}
func TestJobEmptyListAndNullableProjection(t *testing.T) {
	s := &svc.ServiceContext{JobRpc: &jobRPCStub{}}
	list, err := NewListJobsLogic(context.Background(), s).ListJobs(&types.JobListReq{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"items":[]`) {
		t.Fatalf("empty list=%s", data)
	}
	detail, err := NewGetJobLogic(context.Background(), s).GetJob(&types.IDReq{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"executorGroupName":null`) || !strings.Contains(string(data), `"nextTriggerAt":null`) {
		t.Fatalf("missing nulls=%s", data)
	}
}
