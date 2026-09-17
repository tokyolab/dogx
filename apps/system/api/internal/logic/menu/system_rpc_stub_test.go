package menu

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"
	"google.golang.org/grpc"
)

type systemRPCStub struct {
	systemclient.System
	create *systemclient.CreateMenuRequest
	update *systemclient.UpdateMenuRequest
	status *systemclient.UpdateMenuStatusRequest
	id     int64
	item   *systemclient.MenuInfo
	items  []*systemclient.MenuInfo
	err    error
}

func (s *systemRPCStub) CreateMenu(_ context.Context, in *systemclient.CreateMenuRequest, _ ...grpc.CallOption) (*systemclient.CreateMenuResponse, error) {
	s.create = in
	return &systemclient.CreateMenuResponse{Id: 9}, s.err
}
func (s *systemRPCStub) UpdateMenu(_ context.Context, in *systemclient.UpdateMenuRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.update = in
	return &systemclient.EmptyResponse{}, s.err
}
func (s *systemRPCStub) UpdateMenuStatus(_ context.Context, in *systemclient.UpdateMenuStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.status = in
	return &systemclient.EmptyResponse{}, s.err
}
func (s *systemRPCStub) DeleteMenu(_ context.Context, in *systemclient.DeleteMenuRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.id = in.Id
	return &systemclient.EmptyResponse{}, s.err
}
func (s *systemRPCStub) GetMenu(_ context.Context, in *systemclient.GetMenuRequest, _ ...grpc.CallOption) (*systemclient.GetMenuResponse, error) {
	s.id = in.Id
	return &systemclient.GetMenuResponse{Menu: s.item}, s.err
}
func (s *systemRPCStub) ListMenus(context.Context, *systemclient.ListMenusRequest, ...grpc.CallOption) (*systemclient.ListMenusResponse, error) {
	return &systemclient.ListMenusResponse{Items: s.items}, s.err
}
