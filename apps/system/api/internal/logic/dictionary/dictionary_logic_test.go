package dictionary

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/tokyolab/dogx/apps/system/api/internal/svc"
	"github.com/tokyolab/dogx/apps/system/api/internal/types"
	"github.com/tokyolab/dogx/apps/system/rpc/systemclient"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDictionaryHTTPForwardingAndFailures(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		call    func(*svc.ServiceContext) (any, error)
		want    any
		nilCall func(*svc.ServiceContext) error
	}{
		{name: "ListDictionaries", call: func(s *svc.ServiceContext) (any, error) { return NewListDictionariesLogic(ctx, s).ListDictionaries() }, want: &systemclient.ListDictionariesRequest{}, nilCall: nil},
		{name: "GetDictionary", call: func(s *svc.ServiceContext) (any, error) {
			return NewGetDictionaryLogic(ctx, s).GetDictionary(&types.GetDictionaryReq{Id: 7})
		}, want: &systemclient.GetDictionaryRequest{Id: 7}, nilCall: func(s *svc.ServiceContext) error { _, e := NewGetDictionaryLogic(ctx, s).GetDictionary(nil); return e }},
		{name: "CreateDictionary", call: func(s *svc.ServiceContext) (any, error) {
			return NewCreateDictionaryLogic(ctx, s).CreateDictionary(&types.CreateDictionaryReq{Name: "来源", Code: "source", Remark: "说明", Status: 1, IsPublic: true})
		}, want: &systemclient.CreateDictionaryRequest{Name: "来源", Code: "source", Remark: "说明", Status: 1, IsPublic: true}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewCreateDictionaryLogic(ctx, s).CreateDictionary(nil)
			return e
		}},
		{name: "UpdateDictionary", call: func(s *svc.ServiceContext) (any, error) {
			return NewUpdateDictionaryLogic(ctx, s).UpdateDictionary(&types.UpdateDictionaryReq{Id: 7, Name: "来源", Remark: "说明", IsPublic: true})
		}, want: &systemclient.UpdateDictionaryRequest{Id: 7, Name: "来源", Remark: "说明", IsPublic: true}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewUpdateDictionaryLogic(ctx, s).UpdateDictionary(nil)
			return e
		}},
		{name: "UpdateDictionaryStatus", call: func(s *svc.ServiceContext) (any, error) {
			return NewUpdateDictionaryStatusLogic(ctx, s).UpdateDictionaryStatus(&types.UpdateDictionaryStatusReq{Id: 7, Status: 1})
		}, want: &systemclient.UpdateDictionaryStatusRequest{Id: 7, Status: 1}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewUpdateDictionaryStatusLogic(ctx, s).UpdateDictionaryStatus(nil)
			return e
		}},
		{name: "DeleteDictionary", call: func(s *svc.ServiceContext) (any, error) {
			return NewDeleteDictionaryLogic(ctx, s).DeleteDictionary(&types.DeleteDictionaryReq{Id: 7})
		}, want: &systemclient.DeleteDictionaryRequest{Id: 7}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewDeleteDictionaryLogic(ctx, s).DeleteDictionary(nil)
			return e
		}},
		{name: "ListDictionaryItems", call: func(s *svc.ServiceContext) (any, error) {
			return NewListDictionaryItemsLogic(ctx, s).ListDictionaryItems(&types.ListDictionaryItemsReq{DictionaryId: 7})
		}, want: &systemclient.ListDictionaryItemsRequest{DictionaryId: 7}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewListDictionaryItemsLogic(ctx, s).ListDictionaryItems(nil)
			return e
		}},
		{name: "GetDictionaryItem", call: func(s *svc.ServiceContext) (any, error) {
			return NewGetDictionaryItemLogic(ctx, s).GetDictionaryItem(&types.GetDictionaryItemReq{Id: 7})
		}, want: &systemclient.GetDictionaryItemRequest{Id: 7}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewGetDictionaryItemLogic(ctx, s).GetDictionaryItem(nil)
			return e
		}},
		{name: "CreateDictionaryItem", call: func(s *svc.ServiceContext) (any, error) {
			return NewCreateDictionaryItemLogic(ctx, s).CreateDictionaryItem(&types.CreateDictionaryItemReq{DictionaryId: 7, Label: "官网", Value: "web", Sort: 2, Remark: "说明", Status: 1})
		}, want: &systemclient.CreateDictionaryItemRequest{DictionaryId: 7, Label: "官网", Value: "web", Sort: 2, Remark: "说明", Status: 1}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewCreateDictionaryItemLogic(ctx, s).CreateDictionaryItem(nil)
			return e
		}},
		{name: "UpdateDictionaryItem", call: func(s *svc.ServiceContext) (any, error) {
			return NewUpdateDictionaryItemLogic(ctx, s).UpdateDictionaryItem(&types.UpdateDictionaryItemReq{Id: 7, Label: "官网", Sort: 2, Remark: "说明"})
		}, want: &systemclient.UpdateDictionaryItemRequest{Id: 7, Label: "官网", Sort: 2, Remark: "说明"}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewUpdateDictionaryItemLogic(ctx, s).UpdateDictionaryItem(nil)
			return e
		}},
		{name: "UpdateDictionaryItemStatus", call: func(s *svc.ServiceContext) (any, error) {
			return NewUpdateDictionaryItemStatusLogic(ctx, s).UpdateDictionaryItemStatus(&types.UpdateDictionaryItemStatusReq{Id: 7, Status: 1})
		}, want: &systemclient.UpdateDictionaryItemStatusRequest{Id: 7, Status: 1}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewUpdateDictionaryItemStatusLogic(ctx, s).UpdateDictionaryItemStatus(nil)
			return e
		}},
		{name: "DeleteDictionaryItem", call: func(s *svc.ServiceContext) (any, error) {
			return NewDeleteDictionaryItemLogic(ctx, s).DeleteDictionaryItem(&types.DeleteDictionaryItemReq{Id: 7})
		}, want: &systemclient.DeleteDictionaryItemRequest{Id: 7}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewDeleteDictionaryItemLogic(ctx, s).DeleteDictionaryItem(nil)
			return e
		}},
		{name: "ClearDictionaryCache", call: func(s *svc.ServiceContext) (any, error) {
			return NewClearDictionaryCacheLogic(ctx, s).ClearDictionaryCache()
		}, want: &systemclient.ClearDictionaryCacheRequest{}, nilCall: nil},
		{name: "ReadDictionaries", call: func(s *svc.ServiceContext) (any, error) {
			return NewReadDictionariesLogic(ctx, s).ReadDictionaries(&types.ReadDictionariesReq{Codes: []string{"source"}})
		}, want: &systemclient.ReadDictionariesRequest{Codes: []string{"source"}, PublicOnly: false}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewReadDictionariesLogic(ctx, s).ReadDictionaries(nil)
			return e
		}},
		{name: "ReadPublicDictionaries", call: func(s *svc.ServiceContext) (any, error) {
			return NewReadPublicDictionariesLogic(ctx, s).ReadPublicDictionaries(&types.ReadDictionariesReq{Codes: []string{"source"}})
		}, want: &systemclient.ReadDictionariesRequest{Codes: []string{"source"}, PublicOnly: true}, nilCall: func(s *svc.ServiceContext) error {
			_, e := NewReadPublicDictionariesLogic(ctx, s).ReadPublicDictionaries(nil)
			return e
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rpc := &dictionarySystemRPCStub{}
			s := &svc.ServiceContext{SystemRpc: rpc}
			response, err := tc.call(s)
			if err != nil || response == nil || !reflect.DeepEqual(rpc.request, tc.want) {
				t.Fatalf("mapping: response=%+v request=%+v want=%+v err=%v", response, rpc.request, tc.want, err)
			}
			if tc.nilCall != nil && status.Code(tc.nilCall(s)) != codes.InvalidArgument {
				t.Fatal("nil request accepted")
			}
			sentinel := errors.New("RPC failure")
			rpc.err = sentinel
			if _, err := tc.call(s); !errors.Is(err, sentinel) {
				t.Fatalf("RPC error lost: %v", err)
			}
			rpc.err = nil
			rpc.empty = true
			if _, err := tc.call(s); status.Code(err) != codes.Internal {
				t.Fatal("nil RPC result accepted", err)
			}
		})
	}
}
func TestDictionaryHTTPPreservesHistoryAndEmptyArrays(t *testing.T) {
	rpc := &dictionarySystemRPCStub{}
	s := &svc.ServiceContext{SystemRpc: rpc}
	ctx := context.Background()
	out, err := NewReadPublicDictionariesLogic(ctx, s).ReadPublicDictionaries(&types.ReadDictionariesReq{Codes: []string{"source"}})
	if err != nil || len(out.Items) != 1 || len(out.Items[0].Items) != 1 || out.Items[0].Items[0].Status != 0 || out.Items[0].Items[0].Value != "web" {
		t.Fatal(out, err)
	}
	if out := toDictionaryOptions(&systemclient.DictionaryOptions{Code: "empty"}); out.Items == nil {
		t.Fatal("empty items serialized as null")
	}
}
