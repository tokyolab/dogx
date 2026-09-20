package user

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListUserDepartmentOptionsMapsDepartmentsAndErrors(t *testing.T) {
	rpc := &userRPCStub{departmentResponse: &systemclient.ListDepartmentsResponse{Items: []*systemclient.DepartmentInfo{{Id: 7, Name: "研发部", ParentId: 2, Status: 1}}}}
	result, err := NewListUserDepartmentOptionsLogic(context.Background(), &svc.ServiceContext{SystemRpc: rpc}).ListUserDepartmentOptions()
	if err != nil || len(result.Items) != 1 || result.Items[0].Id != 7 || result.Items[0].ParentId != 2 || result.Items[0].Name != "研发部" || rpc.request == nil {
		t.Fatalf("result=%+v error=%v request=%+v", result, err, rpc.request)
	}
	sentinel := errors.New("RPC unavailable")
	if _, err := NewListUserDepartmentOptionsLogic(context.Background(), &svc.ServiceContext{SystemRpc: &userRPCStub{err: sentinel}}).ListUserDepartmentOptions(); !errors.Is(err, sentinel) {
		t.Fatalf("RPC error=%v", err)
	}
	if _, err := NewListUserDepartmentOptionsLogic(context.Background(), &svc.ServiceContext{SystemRpc: &userRPCStub{nilResult: true}}).ListUserDepartmentOptions(); status.Code(err) != codes.Internal {
		t.Fatalf("nil RPC response error=%v", err)
	}
	if _, err := NewListUserDepartmentOptionsLogic(context.Background(), &svc.ServiceContext{SystemRpc: &userRPCStub{departmentResponse: &systemclient.ListDepartmentsResponse{Items: []*systemclient.DepartmentInfo{{}}}}}).ListUserDepartmentOptions(); status.Code(err) != codes.Internal {
		t.Fatalf("invalid department response error=%v", err)
	}
}
