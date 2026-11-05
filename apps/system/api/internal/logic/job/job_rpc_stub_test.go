package job

import (
	"context"
	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"google.golang.org/grpc"
)

type jobRPCStub struct {
	called  string
	request any
	err     error
	invalid bool
}

func (s *jobRPCStub) CreateJob(_ context.Context, in *jobclient.CreateJobRequest, _ ...grpc.CallOption) (*jobclient.CreateJobResponse, error) {
	s.called = "CreateJob"
	s.request = in
	if s.invalid {
		return nil, s.err
	}
	return &jobclient.CreateJobResponse{Id: 7}, s.err
}
func (s *jobRPCStub) UpdateJob(_ context.Context, in *jobclient.UpdateJobRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.called = "UpdateJob"
	s.request = in
	if s.invalid {
		return nil, s.err
	}
	return &jobclient.EmptyResponse{}, s.err
}
func (s *jobRPCStub) UpdateJobStatus(_ context.Context, in *jobclient.UpdateJobStatusRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.called = "UpdateJobStatus"
	s.request = in
	if s.invalid {
		return nil, s.err
	}
	return &jobclient.EmptyResponse{}, s.err
}
func (s *jobRPCStub) DeleteJob(_ context.Context, in *jobclient.DeleteJobRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.called = "DeleteJob"
	s.request = in
	if s.invalid {
		return nil, s.err
	}
	return &jobclient.EmptyResponse{}, s.err
}
func (s *jobRPCStub) GetJob(_ context.Context, in *jobclient.GetJobRequest, _ ...grpc.CallOption) (*jobclient.GetJobResponse, error) {
	s.called = "GetJob"
	s.request = in
	if s.invalid {
		return nil, s.err
	}
	return &jobclient.GetJobResponse{Job: &jobclient.JobInfo{Id: 7}}, s.err
}
func (s *jobRPCStub) ListJobs(_ context.Context, in *jobclient.ListJobsRequest, _ ...grpc.CallOption) (*jobclient.ListJobsResponse, error) {
	s.called = "ListJobs"
	s.request = in
	if s.invalid {
		return nil, s.err
	}
	return &jobclient.ListJobsResponse{}, s.err
}
func (s *jobRPCStub) PreviewJobCron(_ context.Context, in *jobclient.PreviewJobCronRequest, _ ...grpc.CallOption) (*jobclient.PreviewJobCronResponse, error) {
	s.called = "PreviewJobCron"
	s.request = in
	if s.invalid {
		return nil, s.err
	}
	return &jobclient.PreviewJobCronResponse{Times: []string{"2026-10-16T09:00:00+08:00"}}, s.err
}
func (s *jobRPCStub) TriggerJob(_ context.Context, in *jobclient.TriggerJobRequest, _ ...grpc.CallOption) (*jobclient.TriggerJobResponse, error) {
	s.called = "TriggerJob"
	s.request = in
	if s.invalid {
		return nil, s.err
	}
	return &jobclient.TriggerJobResponse{InstanceId: 9}, s.err
}

func (s *jobRPCStub) CreateExecutorGroup(context.Context, *jobclient.CreateExecutorGroupRequest, ...grpc.CallOption) (*jobclient.CreateExecutorGroupResponse, error) {
	panic("unexpected executor group RPC in Job-only test")
}

func (s *jobRPCStub) GetExecutorGroup(context.Context, *jobclient.GetExecutorGroupRequest, ...grpc.CallOption) (*jobclient.GetExecutorGroupResponse, error) {
	panic("unexpected executor group RPC in Job-only test")
}

func (s *jobRPCStub) ListExecutorGroups(context.Context, *jobclient.ListExecutorGroupsRequest, ...grpc.CallOption) (*jobclient.ListExecutorGroupsResponse, error) {
	panic("unexpected executor group RPC in Job-only test")
}

func (s *jobRPCStub) DeleteExecutorGroup(context.Context, *jobclient.DeleteExecutorGroupRequest, ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	panic("unexpected executor group RPC in Job-only test")
}
func (*jobRPCStub) GetInstance(context.Context, *jobclient.GetInstanceRequest, ...grpc.CallOption) (*jobclient.GetInstanceResponse, error) {
	panic("unexpected execution query in Job-only test")
}

func (*jobRPCStub) StopInstance(context.Context, *jobclient.StopInstanceRequest, ...grpc.CallOption) (*jobclient.StopInstanceResponse, error) {
	panic("unexpected instance stop in Job-only test")
}

func (*jobRPCStub) ListInstanceTasks(context.Context, *jobclient.ListInstanceTasksRequest, ...grpc.CallOption) (*jobclient.ListInstanceTasksResponse, error) {
	panic("unexpected execution query in Job-only test")
}
