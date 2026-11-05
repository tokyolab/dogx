package jobgroup

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/tokyolab/dogx/apps/job/rpc/jobclient"
	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestGroupLogicMappingAndErrors(t *testing.T) {
	for _, test := range []struct {
		method string
		call   func(*svc.ServiceContext, bool) (any, error)
		want   proto.Message
	}{
		{"CreateExecutorGroup", func(s *svc.ServiceContext, nilInput bool) (any, error) {
			var req *types.CreateExecutorGroupReq
			if !nilInput {
				req = &types.CreateExecutorGroupReq{Name: " Exact Group ", DiscoveryKey: "/Keep//This/Key"}
			}
			return NewCreateExecutorGroupLogic(context.Background(), s).CreateExecutorGroup(req)
		}, &jobclient.CreateExecutorGroupRequest{Name: " Exact Group ", DiscoveryKey: "/Keep//This/Key"}},
		{"GetExecutorGroup", func(s *svc.ServiceContext, nilInput bool) (any, error) {
			var req *types.IDReq
			if !nilInput {
				req = &types.IDReq{Id: 7}
			}
			return NewGetExecutorGroupLogic(context.Background(), s).GetExecutorGroup(req)
		}, &jobclient.GetExecutorGroupRequest{Id: 7}},
		{"ListExecutorGroups", func(s *svc.ServiceContext, nilInput bool) (any, error) {
			var req *types.ListExecutorGroupsReq
			if !nilInput {
				req = &types.ListExecutorGroupsReq{PageReq: types.PageReq{Page: 2, PageSize: 30}, Name: "_Exact%"}
			}
			return NewListExecutorGroupsLogic(context.Background(), s).ListExecutorGroups(req)
		}, &jobclient.ListExecutorGroupsRequest{Page: 2, PageSize: 30, Name: "_Exact%"}},
		{"DeleteExecutorGroup", func(s *svc.ServiceContext, nilInput bool) (any, error) {
			var req *types.IDReq
			if !nilInput {
				req = &types.IDReq{Id: 7}
			}
			return NewDeleteExecutorGroupLogic(context.Background(), s).DeleteExecutorGroup(req)
		}, &jobclient.DeleteExecutorGroupRequest{Id: 7}},
	} {
		t.Run(test.method, func(t *testing.T) {
			for _, failure := range []error{nil, status.Error(codes.Unavailable, "temporary dependency failure")} {
				stub := validGroupRPCStub()
				stub.err = failure
				_, err := test.call(&svc.ServiceContext{JobRpc: stub}, false)
				if !errors.Is(err, failure) || stub.method != test.method || !proto.Equal(stub.request, test.want) {
					t.Fatalf("method=%s request=%v error=%v", stub.method, stub.request, err)
				}
			}
			stub := &groupRPCStub{}
			if _, err := test.call(&svc.ServiceContext{JobRpc: stub}, false); status.Code(err) != codes.Internal {
				t.Fatalf("nil RPC response error=%v", err)
			}
			stub = &groupRPCStub{}
			if _, err := test.call(&svc.ServiceContext{JobRpc: stub}, true); status.Code(err) != codes.InvalidArgument || stub.calls != 0 {
				t.Fatalf("nil request error=%v calls=%d", err, stub.calls)
			}
		})
	}
}

func validGroupRPCStub() *groupRPCStub {
	return &groupRPCStub{
		create: &jobclient.CreateExecutorGroupResponse{Id: 7},
		get: &jobclient.GetExecutorGroupResponse{Group: &jobclient.ExecutorGroupInfo{
			Id: 7, Name: "Workers", DiscoveryKey: "/workers/rpc", Token: "detail-only-credential",
			CreatedAt: "2026-11-04T08:00:00+08:00", UpdatedAt: "2026-11-04T09:00:00+08:00",
		}},
		list: &jobclient.ListExecutorGroupsResponse{Items: []*jobclient.ExecutorGroupListItem{{
			Id: 7, Name: "Workers", DiscoveryKey: "/workers/rpc",
			CreatedAt: "2026-11-04T08:00:00+08:00", UpdatedAt: "2026-11-04T09:00:00+08:00",
		}}, Total: 1},
		delete: &jobclient.EmptyResponse{},
	}
}

func TestGroupProjectionKeepsCredentialOnlyInDetail(t *testing.T) {
	stub := validGroupRPCStub()
	s := &svc.ServiceContext{JobRpc: stub}
	detail, err := NewGetExecutorGroupLogic(context.Background(), s).GetExecutorGroup(&types.IDReq{Id: 7})
	if err != nil {
		t.Fatal(err)
	}
	if detail.Id != 7 || detail.Name != "Workers" || detail.DiscoveryKey != "/workers/rpc" || detail.Token != "detail-only-credential" || detail.CreatedAt != stub.get.Group.CreatedAt || detail.UpdatedAt != stub.get.Group.UpdatedAt {
		t.Fatal("group details were not preserved")
	}
	list, err := NewListExecutorGroupsLogic(context.Background(), s).ListExecutorGroups(&types.ListExecutorGroupsReq{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "token") || strings.Contains(string(data), "credential") || list.Total != 1 || len(list.Items) != 1 || list.Items[0].CreatedAt != detail.CreatedAt || list.Items[0].UpdatedAt != detail.UpdatedAt {
		t.Fatal("list exposed credentials or changed the projection")
	}
	stub.list = &jobclient.ListExecutorGroupsResponse{}
	list, err = NewListExecutorGroupsLogic(context.Background(), s).ListExecutorGroups(&types.ListExecutorGroupsReq{})
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(list)
	if err != nil || string(data) != `{"items":[],"total":0}` {
		t.Fatalf("empty list=%s error=%v", data, err)
	}
	stub.list.Items = []*jobclient.ExecutorGroupListItem{nil}
	if _, err := NewListExecutorGroupsLogic(context.Background(), s).ListExecutorGroups(&types.ListExecutorGroupsReq{}); status.Code(err) != codes.Internal {
		t.Fatalf("nil list item accepted: %v", err)
	}
	stub.get = &jobclient.GetExecutorGroupResponse{}
	if _, err := NewGetExecutorGroupLogic(context.Background(), s).GetExecutorGroup(&types.IDReq{Id: 7}); status.Code(err) != codes.Internal {
		t.Fatalf("nil group accepted: %v", err)
	}
	stub.create.Id = 0
	if _, err := NewCreateExecutorGroupLogic(context.Background(), s).CreateExecutorGroup(&types.CreateExecutorGroupReq{}); status.Code(err) != codes.Internal {
		t.Fatalf("invalid created ID accepted: %v", err)
	}
}
