package dictionary

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"google.golang.org/grpc"
)

type dictionaryHandlerRPCStub struct {
	systemclient.System
	called string
	err    error
}

func (s *dictionaryHandlerRPCStub) ListDictionaries(context.Context, *systemclient.ListDictionariesRequest, ...grpc.CallOption) (*systemclient.ListDictionariesResponse, error) {
	s.called = "ListDictionaries"
	return &systemclient.ListDictionariesResponse{Items: []*systemclient.DictionaryInfo{{Id: 7, Name: "来源", Code: "source", Status: 1}, nil}}, s.err
}
func (s *dictionaryHandlerRPCStub) GetDictionary(context.Context, *systemclient.GetDictionaryRequest, ...grpc.CallOption) (*systemclient.GetDictionaryResponse, error) {
	s.called = "GetDictionary"
	return &systemclient.GetDictionaryResponse{Dictionary: &systemclient.DictionaryInfo{Id: 7, Name: "来源", Code: "source", Status: 1, IsPublic: true}}, s.err
}
func (s *dictionaryHandlerRPCStub) CreateDictionary(context.Context, *systemclient.CreateDictionaryRequest, ...grpc.CallOption) (*systemclient.CreateDictionaryResponse, error) {
	s.called = "CreateDictionary"
	return &systemclient.CreateDictionaryResponse{Id: 7}, s.err
}
func (s *dictionaryHandlerRPCStub) UpdateDictionary(context.Context, *systemclient.UpdateDictionaryRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "UpdateDictionary"
	return &systemclient.EmptyResponse{}, s.err
}
func (s *dictionaryHandlerRPCStub) UpdateDictionaryStatus(context.Context, *systemclient.UpdateDictionaryStatusRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "UpdateDictionaryStatus"
	return &systemclient.EmptyResponse{}, s.err
}
func (s *dictionaryHandlerRPCStub) DeleteDictionary(context.Context, *systemclient.DeleteDictionaryRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "DeleteDictionary"
	return &systemclient.EmptyResponse{}, s.err
}
func (s *dictionaryHandlerRPCStub) ListDictionaryItems(context.Context, *systemclient.ListDictionaryItemsRequest, ...grpc.CallOption) (*systemclient.ListDictionaryItemsResponse, error) {
	s.called = "ListDictionaryItems"
	return &systemclient.ListDictionaryItemsResponse{Items: []*systemclient.DictionaryItemInfo{{Id: 8, DictionaryId: 7, Label: "官网", Value: "web", Status: 0}, nil}}, s.err
}
func (s *dictionaryHandlerRPCStub) GetDictionaryItem(context.Context, *systemclient.GetDictionaryItemRequest, ...grpc.CallOption) (*systemclient.GetDictionaryItemResponse, error) {
	s.called = "GetDictionaryItem"
	return &systemclient.GetDictionaryItemResponse{Item: &systemclient.DictionaryItemInfo{Id: 8, DictionaryId: 7, Label: "官网", Value: "web", Sort: 2, Status: 0}}, s.err
}
func (s *dictionaryHandlerRPCStub) CreateDictionaryItem(context.Context, *systemclient.CreateDictionaryItemRequest, ...grpc.CallOption) (*systemclient.CreateDictionaryItemResponse, error) {
	s.called = "CreateDictionaryItem"
	return &systemclient.CreateDictionaryItemResponse{Id: 7}, s.err
}
func (s *dictionaryHandlerRPCStub) UpdateDictionaryItem(context.Context, *systemclient.UpdateDictionaryItemRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "UpdateDictionaryItem"
	return &systemclient.EmptyResponse{}, s.err
}
func (s *dictionaryHandlerRPCStub) UpdateDictionaryItemStatus(context.Context, *systemclient.UpdateDictionaryItemStatusRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "UpdateDictionaryItemStatus"
	return &systemclient.EmptyResponse{}, s.err
}
func (s *dictionaryHandlerRPCStub) DeleteDictionaryItem(context.Context, *systemclient.DeleteDictionaryItemRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "DeleteDictionaryItem"
	return &systemclient.EmptyResponse{}, s.err
}
func (s *dictionaryHandlerRPCStub) ClearDictionaryCache(context.Context, *systemclient.ClearDictionaryCacheRequest, ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.called = "ClearDictionaryCache"
	return &systemclient.EmptyResponse{}, s.err
}
func (s *dictionaryHandlerRPCStub) ReadDictionaries(context.Context, *systemclient.ReadDictionariesRequest, ...grpc.CallOption) (*systemclient.ReadDictionariesResponse, error) {
	s.called = "ReadDictionaries"
	return &systemclient.ReadDictionariesResponse{Items: []*systemclient.DictionaryOptions{{Code: "source", Status: 1, Items: []*systemclient.DictionaryOption{{Label: "官网", Value: "web", Status: 0}, nil}}, nil}}, s.err
}
