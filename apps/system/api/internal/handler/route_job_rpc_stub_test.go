package handler

import (
	"context"
	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
)

type routeJobRPCStub struct{ recorder *routeSystemRPCStub }

func (s *routeJobRPCStub) StopInstance(context.Context, *jobclient.StopInstanceRequest, ...grpc.CallOption) (*jobclient.StopInstanceResponse, error) {
	s.record("StopInstance")
	return &jobclient.StopInstanceResponse{InstanceId: 7, Status: 6}, nil
}

func (s *routeJobRPCStub) GetInstance(context.Context, *jobclient.GetInstanceRequest, ...grpc.CallOption) (*jobclient.GetInstanceResponse, error) {
	s.record("GetInstance")
	return &jobclient.GetInstanceResponse{Instance: &jobclient.InstanceInfo{Id: 7, ExecutionConfig: &jobclient.ExecutionConfigInfo{SchemaVersion: 1}}}, nil
}

func (s *routeJobRPCStub) ListInstanceTasks(context.Context, *jobclient.ListInstanceTasksRequest, ...grpc.CallOption) (*jobclient.ListInstanceTasksResponse, error) {
	s.record("ListInstanceTasks")
	return &jobclient.ListInstanceTasksResponse{}, nil
}

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

func (s *routeJobRPCStub) CreateExecutorGroup(_ context.Context, _ *jobclient.CreateExecutorGroupRequest, _ ...grpc.CallOption) (*jobclient.CreateExecutorGroupResponse, error) {
	s.record("CreateExecutorGroup")
	return &jobclient.CreateExecutorGroupResponse{Id: 7}, nil
}

func (s *routeJobRPCStub) GetExecutorGroup(_ context.Context, _ *jobclient.GetExecutorGroupRequest, _ ...grpc.CallOption) (*jobclient.GetExecutorGroupResponse, error) {
	s.record("GetExecutorGroup")
	return &jobclient.GetExecutorGroupResponse{Group: &jobclient.ExecutorGroupInfo{Id: 7}}, nil
}

func (s *routeJobRPCStub) ListExecutorGroups(_ context.Context, _ *jobclient.ListExecutorGroupsRequest, _ ...grpc.CallOption) (*jobclient.ListExecutorGroupsResponse, error) {
	s.record("ListExecutorGroups")
	return &jobclient.ListExecutorGroupsResponse{}, nil
}

func (s *routeJobRPCStub) DeleteExecutorGroup(_ context.Context, _ *jobclient.DeleteExecutorGroupRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.record("DeleteExecutorGroup")
	return &jobclient.EmptyResponse{}, nil
}
