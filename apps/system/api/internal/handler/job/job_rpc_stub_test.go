package job

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type handlerJobRPCStub struct {
	calls   int
	method  string
	request proto.Message
	err     error
	create  *jobclient.CreateJobRequest
	trigger *jobclient.TriggerJobRequest
}

func (s *handlerJobRPCStub) record(method string, request proto.Message) {
	s.calls++
	s.method, s.request = method, request
}

func (s *handlerJobRPCStub) CreateJob(_ context.Context, in *jobclient.CreateJobRequest, _ ...grpc.CallOption) (*jobclient.CreateJobResponse, error) {
	s.record("CreateJob", in)
	s.create = in
	return &jobclient.CreateJobResponse{Id: 7}, s.err
}
func (s *handlerJobRPCStub) TriggerJob(_ context.Context, in *jobclient.TriggerJobRequest, _ ...grpc.CallOption) (*jobclient.TriggerJobResponse, error) {
	s.record("TriggerJob", in)
	s.trigger = in
	return &jobclient.TriggerJobResponse{InstanceId: 9}, s.err
}

func (s *handlerJobRPCStub) ListJobs(_ context.Context, in *jobclient.ListJobsRequest, _ ...grpc.CallOption) (*jobclient.ListJobsResponse, error) {
	s.record("ListJobs", in)
	return &jobclient.ListJobsResponse{}, s.err
}
func (s *handlerJobRPCStub) GetJob(_ context.Context, in *jobclient.GetJobRequest, _ ...grpc.CallOption) (*jobclient.GetJobResponse, error) {
	s.record("GetJob", in)
	return &jobclient.GetJobResponse{Job: &jobclient.JobInfo{Id: 7}}, s.err
}
func (s *handlerJobRPCStub) UpdateJob(_ context.Context, in *jobclient.UpdateJobRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.record("UpdateJob", in)
	return &jobclient.EmptyResponse{}, s.err
}
func (s *handlerJobRPCStub) UpdateJobStatus(_ context.Context, in *jobclient.UpdateJobStatusRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.record("UpdateJobStatus", in)
	return &jobclient.EmptyResponse{}, s.err
}
func (s *handlerJobRPCStub) DeleteJob(_ context.Context, in *jobclient.DeleteJobRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.record("DeleteJob", in)
	return &jobclient.EmptyResponse{}, s.err
}
func (s *handlerJobRPCStub) PreviewJobCron(_ context.Context, in *jobclient.PreviewJobCronRequest, _ ...grpc.CallOption) (*jobclient.PreviewJobCronResponse, error) {
	s.record("PreviewJobCron", in)
	return &jobclient.PreviewJobCronResponse{Times: []string{"2026-10-16T12:01:00+08:00"}}, s.err
}

var _ jobclient.Job = (*handlerJobRPCStub)(nil)
