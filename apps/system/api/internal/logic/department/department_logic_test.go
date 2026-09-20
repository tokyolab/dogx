package department

import (
	"context"
	"errors"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDepartmentHTTPLogicMapsRequestsAndResponses(t *testing.T) {
	rpc := &departmentSystemRPCStub{
		createResponse: &systemclient.CreateDepartmentResponse{Id: 61},
		getResponse: &systemclient.GetDepartmentResponse{Department: &systemclient.DepartmentInfo{
			Id: 61, ParentId: 4, Name: "研发部", Sort: 2, Status: 1, Remark: "备注", CreatedAt: "2026-09-20T00:00:00Z", UpdatedAt: "2026-09-20T01:00:00Z",
		}},
		listResponse: &systemclient.ListDepartmentsResponse{Items: []*systemclient.DepartmentInfo{{
			Id: 61, ParentId: 4, Name: "研发部", Sort: 2, Status: 1, Remark: "备注", CreatedAt: "2026-09-20T00:00:00Z", UpdatedAt: "2026-09-20T01:00:00Z",
		}}},
	}
	ctx := context.Background()
	svcCtx := &svc.ServiceContext{SystemRpc: rpc}
	created, err := NewCreateDepartmentLogic(ctx, svcCtx).CreateDepartment(&types.CreateDepartmentReq{ParentId: 4, Name: "研发部", Sort: 2, Remark: "备注", Status: 1})
	if err != nil || created.Id != 61 || rpc.createRequest.ParentId != 4 || rpc.createRequest.Name != "研发部" || rpc.createRequest.Status != 1 {
		t.Fatalf("create result=%+v error=%v request=%+v", created, err, rpc.createRequest)
	}
	if _, err := NewUpdateDepartmentLogic(ctx, svcCtx).UpdateDepartment(&types.UpdateDepartmentReq{Id: 61, ParentId: 0, Name: "平台组", Sort: 3, Remark: ""}); err != nil || rpc.updateRequest.Id != 61 || rpc.updateRequest.ParentId != 0 || rpc.updateRequest.Name != "平台组" {
		t.Fatalf("update error=%v request=%+v", err, rpc.updateRequest)
	}
	if _, err := NewUpdateDepartmentStatusLogic(ctx, svcCtx).UpdateDepartmentStatus(&types.UpdateDepartmentStatusReq{Id: 61, Status: 0}); err != nil || rpc.statusRequest.Id != 61 || rpc.statusRequest.Status != 0 {
		t.Fatalf("status error=%v request=%+v", err, rpc.statusRequest)
	}
	if _, err := NewDeleteDepartmentLogic(ctx, svcCtx).DeleteDepartment(&types.IDReq{Id: 61}); err != nil || rpc.deleteRequest.Id != 61 {
		t.Fatalf("delete error=%v request=%+v", err, rpc.deleteRequest)
	}
	item, err := NewGetDepartmentLogic(ctx, svcCtx).GetDepartment(&types.IDReq{Id: 61})
	if err != nil || item.Id != 61 || item.ParentId != 4 || item.Name != "研发部" || item.CreatedAt == "" {
		t.Fatalf("get item=%+v error=%v", item, err)
	}
	list, err := NewListDepartmentsLogic(ctx, svcCtx).ListDepartments()
	if err != nil || len(list.Items) != 1 || list.Items[0].Name != "研发部" || rpc.listRequest == nil {
		t.Fatalf("list=%+v error=%v request=%+v", list, err, rpc.listRequest)
	}
}

func TestDepartmentHTTPLogicRejectsNilRequestsAndPropagatesRPCErrors(t *testing.T) {
	ctx := context.Background()
	rpc := &departmentSystemRPCStub{}
	svcCtx := &svc.ServiceContext{SystemRpc: rpc}
	if _, err := NewCreateDepartmentLogic(ctx, svcCtx).CreateDepartment(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("create nil error=%v", err)
	}
	if _, err := NewUpdateDepartmentLogic(ctx, svcCtx).UpdateDepartment(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("update nil error=%v", err)
	}
	if _, err := NewUpdateDepartmentStatusLogic(ctx, svcCtx).UpdateDepartmentStatus(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status nil error=%v", err)
	}
	if _, err := NewDeleteDepartmentLogic(ctx, svcCtx).DeleteDepartment(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("delete nil error=%v", err)
	}
	if _, err := NewGetDepartmentLogic(ctx, svcCtx).GetDepartment(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("get nil error=%v", err)
	}
	sentinel := errors.New("RPC unavailable")
	rpc.err = sentinel
	if _, err := NewCreateDepartmentLogic(ctx, svcCtx).CreateDepartment(&types.CreateDepartmentReq{}); !errors.Is(err, sentinel) {
		t.Fatalf("create error=%v", err)
	}
	if _, err := NewUpdateDepartmentLogic(ctx, svcCtx).UpdateDepartment(&types.UpdateDepartmentReq{Id: 1}); !errors.Is(err, sentinel) {
		t.Fatalf("update error=%v", err)
	}
	if _, err := NewUpdateDepartmentStatusLogic(ctx, svcCtx).UpdateDepartmentStatus(&types.UpdateDepartmentStatusReq{Id: 1}); !errors.Is(err, sentinel) {
		t.Fatalf("status error=%v", err)
	}
	if _, err := NewDeleteDepartmentLogic(ctx, svcCtx).DeleteDepartment(&types.IDReq{Id: 1}); !errors.Is(err, sentinel) {
		t.Fatalf("delete error=%v", err)
	}
	if _, err := NewGetDepartmentLogic(ctx, svcCtx).GetDepartment(&types.IDReq{Id: 1}); !errors.Is(err, sentinel) {
		t.Fatalf("get error=%v", err)
	}
	if _, err := NewListDepartmentsLogic(ctx, svcCtx).ListDepartments(); !errors.Is(err, sentinel) {
		t.Fatalf("list error=%v", err)
	}
}

func TestDepartmentHTTPLogicRejectsInvalidRPCResponses(t *testing.T) {
	ctx := context.Background()
	validCreate := &types.CreateDepartmentReq{Name: "部门"}
	if _, err := NewCreateDepartmentLogic(ctx, &svc.ServiceContext{SystemRpc: &departmentSystemRPCStub{}}).CreateDepartment(validCreate); status.Code(err) != codes.Internal {
		t.Fatalf("invalid create response error=%v", err)
	}
	if _, err := NewGetDepartmentLogic(ctx, &svc.ServiceContext{SystemRpc: &departmentSystemRPCStub{getResponse: &systemclient.GetDepartmentResponse{}}}).GetDepartment(&types.IDReq{Id: 1}); status.Code(err) != codes.Internal {
		t.Fatalf("invalid get response error=%v", err)
	}
	if _, err := NewListDepartmentsLogic(ctx, &svc.ServiceContext{SystemRpc: &departmentSystemRPCStub{listResponse: &systemclient.ListDepartmentsResponse{Items: []*systemclient.DepartmentInfo{nil}}}}).ListDepartments(); status.Code(err) != codes.Internal {
		t.Fatalf("invalid list response error=%v", err)
	}
	for _, item := range []*systemclient.DepartmentInfo{nil, {}, {Name: "missing id"}} {
		if _, err := ToDepartmentItem(item); status.Code(err) != codes.Internal {
			t.Fatalf("item=%+v error=%v", item, err)
		}
	}
}
