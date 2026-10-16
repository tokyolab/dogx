package handler

import (
	"context"
	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
)

type routeJobRPCStub struct{ recorder *routeSystemRPCStub }

func newRouteJobRPC(rpc systemclient.System) jobclient.Job {
	recorder, _ := rpc.(*routeSystemRPCStub)
	return &routeJobRPCStub{recorder: recorder}
}
func (s *routeJobRPCStub) record(name string) {
	if s.recorder != nil {
		s.recorder.called = name
		*s.recorder.order = append(*s.recorder.order, "rpc")
	}
}
func (s *routeJobRPCStub) ListJobs(_ context.Context, _ *jobclient.ListJobsRequest, _ ...grpc.CallOption) (*jobclient.ListJobsResponse, error) {
	s.record("ListJobs")
	return &jobclient.ListJobsResponse{}, nil
}
func (s *routeJobRPCStub) GetJob(_ context.Context, _ *jobclient.GetJobRequest, _ ...grpc.CallOption) (*jobclient.GetJobResponse, error) {
	s.record("GetJob")
	return &jobclient.GetJobResponse{Job: &jobclient.JobInfo{Id: 7}}, nil
}
func (s *routeJobRPCStub) CreateJob(_ context.Context, _ *jobclient.CreateJobRequest, _ ...grpc.CallOption) (*jobclient.CreateJobResponse, error) {
	s.record("CreateJob")
	return &jobclient.CreateJobResponse{Id: 7}, nil
}
func (s *routeJobRPCStub) UpdateJob(_ context.Context, _ *jobclient.UpdateJobRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.record("UpdateJob")
	return &jobclient.EmptyResponse{}, nil
}
func (s *routeJobRPCStub) UpdateJobStatus(_ context.Context, _ *jobclient.UpdateJobStatusRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.record("UpdateJobStatus")
	return &jobclient.EmptyResponse{}, nil
}
func (s *routeJobRPCStub) DeleteJob(_ context.Context, _ *jobclient.DeleteJobRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.record("DeleteJob")
	return &jobclient.EmptyResponse{}, nil
}
func (s *routeJobRPCStub) PreviewJobCron(_ context.Context, _ *jobclient.PreviewJobCronRequest, _ ...grpc.CallOption) (*jobclient.PreviewJobCronResponse, error) {
	s.record("PreviewJobCron")
	return &jobclient.PreviewJobCronResponse{}, nil
}
func (s *routeJobRPCStub) TriggerJob(_ context.Context, _ *jobclient.TriggerJobRequest, _ ...grpc.CallOption) (*jobclient.TriggerJobResponse, error) {
	s.record("TriggerJob")
	return &jobclient.TriggerJobResponse{InstanceId: 9}, nil
}
