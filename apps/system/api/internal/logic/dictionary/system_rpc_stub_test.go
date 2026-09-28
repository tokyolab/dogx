package dictionary

import (
	"context"

	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"google.golang.org/grpc"
)

type dictionarySystemRPCStub struct {
	systemclient.System
	err     error
	empty   bool
	request any
}

func (s *dictionarySystemRPCStub) ListDictionaries(_ context.Context, in *systemclient.ListDictionariesRequest, _ ...grpc.CallOption) (*systemclient.ListDictionariesResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.ListDictionariesResponse{Items: []*systemclient.DictionaryInfo{{Id: 7, Name: "来源", Code: "source", Status: 1}, nil}}, nil
}
func (s *dictionarySystemRPCStub) GetDictionary(_ context.Context, in *systemclient.GetDictionaryRequest, _ ...grpc.CallOption) (*systemclient.GetDictionaryResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.GetDictionaryResponse{Dictionary: &systemclient.DictionaryInfo{Id: 7, Name: "来源", Code: "source", Status: 1, IsPublic: true}}, nil
}
func (s *dictionarySystemRPCStub) CreateDictionary(_ context.Context, in *systemclient.CreateDictionaryRequest, _ ...grpc.CallOption) (*systemclient.CreateDictionaryResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.CreateDictionaryResponse{Id: 7}, nil
}
func (s *dictionarySystemRPCStub) UpdateDictionary(_ context.Context, in *systemclient.UpdateDictionaryRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.EmptyResponse{}, nil
}
func (s *dictionarySystemRPCStub) UpdateDictionaryStatus(_ context.Context, in *systemclient.UpdateDictionaryStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.EmptyResponse{}, nil
}
func (s *dictionarySystemRPCStub) DeleteDictionary(_ context.Context, in *systemclient.DeleteDictionaryRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.EmptyResponse{}, nil
}
func (s *dictionarySystemRPCStub) ListDictionaryItems(_ context.Context, in *systemclient.ListDictionaryItemsRequest, _ ...grpc.CallOption) (*systemclient.ListDictionaryItemsResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.ListDictionaryItemsResponse{Items: []*systemclient.DictionaryItemInfo{{Id: 8, DictionaryId: 7, Label: "官网", Value: "web", Status: 0}, nil}}, nil
}
func (s *dictionarySystemRPCStub) GetDictionaryItem(_ context.Context, in *systemclient.GetDictionaryItemRequest, _ ...grpc.CallOption) (*systemclient.GetDictionaryItemResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.GetDictionaryItemResponse{Item: &systemclient.DictionaryItemInfo{Id: 8, DictionaryId: 7, Label: "官网", Value: "web", Sort: 2, Status: 0}}, nil
}
func (s *dictionarySystemRPCStub) CreateDictionaryItem(_ context.Context, in *systemclient.CreateDictionaryItemRequest, _ ...grpc.CallOption) (*systemclient.CreateDictionaryItemResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.CreateDictionaryItemResponse{Id: 7}, nil
}
func (s *dictionarySystemRPCStub) UpdateDictionaryItem(_ context.Context, in *systemclient.UpdateDictionaryItemRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.EmptyResponse{}, nil
}
func (s *dictionarySystemRPCStub) UpdateDictionaryItemStatus(_ context.Context, in *systemclient.UpdateDictionaryItemStatusRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.EmptyResponse{}, nil
}
func (s *dictionarySystemRPCStub) DeleteDictionaryItem(_ context.Context, in *systemclient.DeleteDictionaryItemRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.EmptyResponse{}, nil
}
func (s *dictionarySystemRPCStub) ClearDictionaryCache(_ context.Context, in *systemclient.ClearDictionaryCacheRequest, _ ...grpc.CallOption) (*systemclient.EmptyResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.EmptyResponse{}, nil
}
func (s *dictionarySystemRPCStub) ReadDictionaries(_ context.Context, in *systemclient.ReadDictionariesRequest, _ ...grpc.CallOption) (*systemclient.ReadDictionariesResponse, error) {
	s.request = in
	if s.err != nil {
		return nil, s.err
	}
	if s.empty {
		return nil, nil
	}
	return &systemclient.ReadDictionariesResponse{Items: []*systemclient.DictionaryOptions{{Code: "source", Status: 1, Items: []*systemclient.DictionaryOption{{Label: "官网", Value: "web", Status: 0}, nil}}, nil}}, nil
}
