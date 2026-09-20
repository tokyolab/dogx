package department

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
)

type departmentSystemRPCStub struct {
	systemclient.System
	createRequest  *systemclient.CreateDepartmentRequest
	updateRequest  *systemclient.UpdateDepartmentRequest
	statusRequest  *systemclient.UpdateDepartmentStatusRequest
	deleteRequest  *systemclient.DeleteDepartmentRequest
	getRequest     *systemclient.GetDepartmentRequest
	listRequest    *systemclient.ListDepartmentsRequest
	createResponse *systemclient.CreateDepartmentResponse
	getResponse    *systemclient.GetDepartmentResponse
	listResponse   *systemclient.ListDepartmentsResponse
	err            error
}

func (s *departmentSystemRPCStub) CreateDepartment(_ context.Context, request *systemclient.CreateDepartmentRequest, _ ...grpc.CallOption) (*systemclient.CreateDepartmentResponse, error) {
	s.createRequest = request
	return s.createResponse, s.err
}

func (s *departmentSystemRPCStub) UpdateDepartment(_ context.Context, request *systemclient.UpdateDepartmentRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.updateRequest = request
	return &systemclient.EmptyResponse{}, s.err
}

func (s *departmentSystemRPCStub) UpdateDepartmentStatus(_ context.Context, request *systemclient.UpdateDepartmentStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.statusRequest = request
	return &systemclient.EmptyResponse{}, s.err
}

func (s *departmentSystemRPCStub) DeleteDepartment(_ context.Context, request *systemclient.DeleteDepartmentRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.deleteRequest = request
	return &systemclient.EmptyResponse{}, s.err
}

func (s *departmentSystemRPCStub) GetDepartment(_ context.Context, request *systemclient.GetDepartmentRequest, _ ...grpc.CallOption) (*systemclient.GetDepartmentResponse, error) {
	s.getRequest = request
	return s.getResponse, s.err
}

func (s *departmentSystemRPCStub) ListDepartments(_ context.Context, request *systemclient.ListDepartmentsRequest, _ ...grpc.CallOption) (*systemclient.ListDepartmentsResponse, error) {
	s.listRequest = request
	return s.listResponse, s.err
}

var _ systemclient.System = (*departmentSystemRPCStub)(nil)
