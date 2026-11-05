package jobgroup

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type groupRPCStub struct {
	jobclient.Job
	method  string
	request proto.Message
	calls   int
	err     error
	create  *jobclient.CreateExecutorGroupResponse
	get     *jobclient.GetExecutorGroupResponse
	list    *jobclient.ListExecutorGroupsResponse
	delete  *jobclient.EmptyResponse
}

func (s *groupRPCStub) record(method string, request proto.Message) {
	s.calls++
	s.method, s.request = method, request
}

func (s *groupRPCStub) CreateExecutorGroup(_ context.Context, req *jobclient.CreateExecutorGroupRequest, _ ...grpc.CallOption) (*jobclient.CreateExecutorGroupResponse, error) {
	s.record("CreateExecutorGroup", req)
	return s.create, s.err
}

func (s *groupRPCStub) GetExecutorGroup(_ context.Context, req *jobclient.GetExecutorGroupRequest, _ ...grpc.CallOption) (*jobclient.GetExecutorGroupResponse, error) {
	s.record("GetExecutorGroup", req)
	return s.get, s.err
}

func (s *groupRPCStub) ListExecutorGroups(_ context.Context, req *jobclient.ListExecutorGroupsRequest, _ ...grpc.CallOption) (*jobclient.ListExecutorGroupsResponse, error) {
	s.record("ListExecutorGroups", req)
	return s.list, s.err
}

func (s *groupRPCStub) DeleteExecutorGroup(_ context.Context, req *jobclient.DeleteExecutorGroupRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.record("DeleteExecutorGroup", req)
	return s.delete, s.err
}

var _ jobclient.Job = (*groupRPCStub)(nil)
