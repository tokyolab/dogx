package jobgroup

import (
	"context"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type handlerGroupRPCStub struct {
	jobclient.Job
	calls   int
	method  string
	request proto.Message
	err     error
}

func (s *handlerGroupRPCStub) record(method string, request proto.Message) {
	s.calls++
	s.method, s.request = method, request
}

func (s *handlerGroupRPCStub) CreateExecutorGroup(_ context.Context, req *jobclient.CreateExecutorGroupRequest, _ ...grpc.CallOption) (*jobclient.CreateExecutorGroupResponse, error) {
	s.record("CreateExecutorGroup", req)
	return &jobclient.CreateExecutorGroupResponse{Id: 7}, s.err
}

func (s *handlerGroupRPCStub) GetExecutorGroup(_ context.Context, req *jobclient.GetExecutorGroupRequest, _ ...grpc.CallOption) (*jobclient.GetExecutorGroupResponse, error) {
	s.record("GetExecutorGroup", req)
	return &jobclient.GetExecutorGroupResponse{Group: &jobclient.ExecutorGroupInfo{
		Id: 7, Name: "Workers", DiscoveryKey: "/workers/rpc", Token: "detail-only-credential",
		CreatedAt: "created", UpdatedAt: "updated",
	}}, s.err
}

func (s *handlerGroupRPCStub) ListExecutorGroups(_ context.Context, req *jobclient.ListExecutorGroupsRequest, _ ...grpc.CallOption) (*jobclient.ListExecutorGroupsResponse, error) {
	s.record("ListExecutorGroups", req)
	return &jobclient.ListExecutorGroupsResponse{Items: []*jobclient.ExecutorGroupListItem{{
		Id: 7, Name: "Workers", DiscoveryKey: "/workers/rpc", CreatedAt: "created", UpdatedAt: "updated",
	}}, Total: 1}, s.err
}

func (s *handlerGroupRPCStub) DeleteExecutorGroup(_ context.Context, req *jobclient.DeleteExecutorGroupRequest, _ ...grpc.CallOption) (*jobclient.EmptyResponse, error) {
	s.record("DeleteExecutorGroup", req)
	return &jobclient.EmptyResponse{}, s.err
}

var _ jobclient.Job = (*handlerGroupRPCStub)(nil)
