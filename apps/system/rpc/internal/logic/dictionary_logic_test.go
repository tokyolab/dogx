package logic

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tokyolab/dogx/apps/system/internal/dictcache"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"github.com/tokyolab/dogx/apps/system/rpc/internal/svc"
	"github.com/tokyolab/dogx/apps/system/rpc/types/system"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func dictionaryFixture() (*svc.ServiceContext, *dictionaryRepositoryStub, *dictionaryItemRepositoryStub, *dictionaryCacheStub) {
	d := &dictionaryRepositoryStub{item: &model.Dictionary{Base: model.Base{ID: 7}, Name: "来源", Code: "source", Status: 1}}
	i := &dictionaryItemRepositoryStub{item: &model.DictionaryItem{Base: model.Base{ID: 8}, DictionaryID: 7, Label: "官网", Value: "web", Status: 1}}
	c := &dictionaryCacheStub{}
	return &svc.ServiceContext{DictionaryRepo: d, DictionaryItemRepo: i, DictionaryCache: c}, d, i, c
}
func TestDictionaryMutationsCommitBeforeInvalidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		item bool
		call func(*svc.ServiceContext) error
	}{
		{"CreateDictionary", false, func(s *svc.ServiceContext) error {
			_, err := NewCreateDictionaryLogic(ctx, s).CreateDictionary(&system.CreateDictionaryRequest{Name: " 字典 ", Code: "source", Status: 1})
			return err
		}},
		{"UpdateDictionary", false, func(s *svc.ServiceContext) error {
			_, err := NewUpdateDictionaryLogic(ctx, s).UpdateDictionary(&system.UpdateDictionaryRequest{Id: 7, Name: " 字典 ", IsPublic: true})
			return err
		}},
		{"UpdateDictionaryStatus", false, func(s *svc.ServiceContext) error {
			_, err := NewUpdateDictionaryStatusLogic(ctx, s).UpdateDictionaryStatus(&system.UpdateDictionaryStatusRequest{Id: 7, Status: 0})
			return err
		}},
		{"DeleteDictionary", false, func(s *svc.ServiceContext) error {
			_, err := NewDeleteDictionaryLogic(ctx, s).DeleteDictionary(&system.DeleteDictionaryRequest{Id: 7})
			return err
		}},
		{"CreateDictionaryItem", true, func(s *svc.ServiceContext) error {
			_, err := NewCreateDictionaryItemLogic(ctx, s).CreateDictionaryItem(&system.CreateDictionaryItemRequest{DictionaryId: 7, Label: " 官网 ", Value: "web", Status: 1})
			return err
		}},
		{"UpdateDictionaryItem", true, func(s *svc.ServiceContext) error {
			_, err := NewUpdateDictionaryItemLogic(ctx, s).UpdateDictionaryItem(&system.UpdateDictionaryItemRequest{Id: 7, Label: " 新标签 ", Sort: 2})
			return err
		}},
		{"UpdateDictionaryItemStatus", true, func(s *svc.ServiceContext) error {
			_, err := NewUpdateDictionaryItemStatusLogic(ctx, s).UpdateDictionaryItemStatus(&system.UpdateDictionaryItemStatusRequest{Id: 7, Status: 0})
			return err
		}},
		{"DeleteDictionaryItem", true, func(s *svc.ServiceContext) error {
			_, err := NewDeleteDictionaryItemLogic(ctx, s).DeleteDictionaryItem(&system.DeleteDictionaryItemRequest{Id: 7})
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, phase := range []string{"success", "write failure", "cache failure", "lookup failure"} {
				t.Run(phase, func(t *testing.T) {
					sc, d, i, c := dictionaryFixture()
					sentinel := errors.New("dependency unavailable")
					if phase == "write failure" {
						d.writeErr = sentinel
						i.writeErr = sentinel
					}
					if phase == "cache failure" {
						c.invalidateErr = sentinel
					}
					if phase == "lookup failure" {
						if tc.name == "CreateDictionary" {
							return
						}
						d.err = sentinel
						i.err = sentinel
					}
					err := tc.call(sc)
					if phase == "success" && err != nil {
						t.Fatal(err)
					}
					if phase != "success" && err == nil {
						t.Fatal("failure reported as success")
					}
					if phase == "lookup failure" {
						if d.writes+i.writes != 0 || len(c.invalidated) != 0 {
							t.Fatal("lookup failure caused mutation")
						}
						return
					}
					if tc.item && i.writes != 1 || !tc.item && d.writes != 1 {
						t.Fatal("expected exactly one database write")
					}
					if phase == "write failure" {
						if !errors.Is(err, sentinel) || len(c.invalidated) != 0 {
							t.Fatalf("write failure invalidated cache: %v", err)
						}
					} else if len(c.invalidated) != 1 || c.invalidated[0] != "source" {
						t.Fatal("wrong cache invalidated")
					}
					if phase == "cache failure" && !strings.Contains(err.Error(), "数据已保存") {
						t.Fatalf("must explain committed state: %v", err)
					}
				})
			}
		})
	}
}
func TestDictionaryValidationBeforeConversion(t *testing.T) {
	sc, _, _, _ := dictionaryFixture()
	ctx := context.Background()
	calls := []func() error{
		func() error { _, e := NewCreateDictionaryLogic(ctx, sc).CreateDictionary(nil); return e },
		func() error { _, e := NewUpdateDictionaryLogic(ctx, sc).UpdateDictionary(nil); return e },
		func() error { _, e := NewUpdateDictionaryStatusLogic(ctx, sc).UpdateDictionaryStatus(nil); return e },
		func() error { _, e := NewDeleteDictionaryLogic(ctx, sc).DeleteDictionary(nil); return e },
		func() error { _, e := NewCreateDictionaryItemLogic(ctx, sc).CreateDictionaryItem(nil); return e },
		func() error { _, e := NewUpdateDictionaryItemLogic(ctx, sc).UpdateDictionaryItem(nil); return e },
		func() error {
			_, e := NewUpdateDictionaryItemStatusLogic(ctx, sc).UpdateDictionaryItemStatus(nil)
			return e
		},
		func() error { _, e := NewDeleteDictionaryItemLogic(ctx, sc).DeleteDictionaryItem(nil); return e },
		func() error {
			_, e := NewGetDictionaryLogic(ctx, sc).GetDictionary(&system.GetDictionaryRequest{})
			return e
		},
		func() error { _, e := NewGetDictionaryItemLogic(ctx, sc).GetDictionaryItem(nil); return e },
		func() error { _, e := NewListDictionaryItemsLogic(ctx, sc).ListDictionaryItems(nil); return e },
		func() error {
			_, e := NewCreateDictionaryLogic(ctx, sc).CreateDictionary(&system.CreateDictionaryRequest{Name: "ok", Code: "OK"})
			return e
		},
		func() error {
			_, e := NewCreateDictionaryLogic(ctx, sc).CreateDictionary(&system.CreateDictionaryRequest{Name: strings.Repeat("中", 129), Code: "ok"})
			return e
		},
		func() error {
			_, e := NewCreateDictionaryItemLogic(ctx, sc).CreateDictionaryItem(&system.CreateDictionaryItemRequest{DictionaryId: 7, Label: "ok", Value: " "})
			return e
		},
		func() error {
			_, e := NewUpdateDictionaryItemLogic(ctx, sc).UpdateDictionaryItem(&system.UpdateDictionaryItemRequest{Id: 7, Label: "ok", Sort: -1})
			return e
		},
	}
	for _, call := range calls {
		if err := call(); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid request: %v", err)
		}
	}
	for _, n := range []int32{-1, 2, 65536, 65537} {
		if _, err := NewUpdateDictionaryStatusLogic(ctx, sc).UpdateDictionaryStatus(&system.UpdateDictionaryStatusRequest{Id: 7, Status: n}); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
		if _, err := NewUpdateDictionaryItemStatusLogic(ctx, sc).UpdateDictionaryItemStatus(&system.UpdateDictionaryItemStatusRequest{Id: 7, Status: n}); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	if !validDictionaryFields(strings.Repeat("中", 128), "") || validDictionaryFields(" ", "") {
		t.Fatal("incorrect Unicode validation")
	}
}
func TestReadDictionariesBatchCacheAndPublicBoundary(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"miss", "partial", "hit", "read failure", "fill failure"} {
		for _, public := range []bool{false, true} {
			t.Run(mode+string(rune('0'+boolInt(public))), func(t *testing.T) {
				sc, d, i, c := dictionaryFixture()
				publicEntry := dictcache.Entry{Status: 0, IsPublic: true, Items: []dictcache.Item{{Label: "旧官网", Value: "web", Status: 0}}}
				internalEntry := dictcache.Entry{Status: 1, IsPublic: false, Items: []dictcache.Item{}}
				if mode == "partial" {
					c.entries = map[string]dictcache.Entry{"source": publicEntry}
				}
				if mode == "hit" {
					c.entries = map[string]dictcache.Entry{"source": publicEntry, "internal": internalEntry}
				}
				d.items = []model.Dictionary{{Base: model.Base{ID: 7}, Code: "source", Status: 0, IsPublic: true}, {Base: model.Base{ID: 8}, Code: "internal", Status: 1}}
				i.items = []model.DictionaryItem{{DictionaryID: 7, Label: "旧官网", Value: "web", Status: 0}}
				if mode == "partial" {
					d.items = d.items[1:]
					i.items = nil
				}
				if mode == "read failure" {
					c.readErr = errors.New("redis down")
				}
				if mode == "fill failure" {
					c.putErr = errors.New("redis down")
				}
				out, err := NewReadDictionariesLogic(ctx, sc).ReadDictionaries(&system.ReadDictionariesRequest{Codes: []string{"source", "internal", "source"}, PublicOnly: public})
				if err != nil {
					t.Fatal(err)
				}
				want := 2
				if public {
					want = 1
				}
				if len(out.Items) != want || out.Items[0].Code != "source" || out.Items[0].Status != 0 || len(out.Items[0].Items) != 1 || out.Items[0].Items[0].Status != 0 {
					t.Fatalf("lost history or leaked private data: %+v", out)
				}
				if mode == "hit" {
					if d.reads+i.reads != 0 {
						t.Fatal("cache hit read database")
					}
				} else if d.reads != 1 || i.reads != 1 {
					t.Fatal("not a bounded batch query")
				}
				if mode == "partial" && (len(d.codes) != 1 || d.codes[0] != "internal" || len(i.ids) != 1 || i.ids[0] != 8 || c.puts != 1) {
					t.Fatalf("partial hit queried or refilled cached dictionaries: codes=%v ids=%v puts=%d", d.codes, i.ids, c.puts)
				}
			})
		}
	}
	sc, d, i, c := dictionaryFixture()
	for _, req := range []*system.ReadDictionariesRequest{nil, {}, {Codes: make([]string, 51)}, {Codes: []string{"UPPER"}}} {
		if _, err := NewReadDictionariesLogic(ctx, sc).ReadDictionaries(req); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	d.items = []model.Dictionary{{Base: model.Base{ID: 7}, Code: "empty", Status: 1}}
	out, err := NewReadDictionariesLogic(ctx, sc).ReadDictionaries(&system.ReadDictionariesRequest{Codes: []string{"empty", "missing"}})
	if err != nil || len(out.Items) != 1 || out.Items[0].Items == nil || c.puts != 1 {
		t.Fatalf("empty/missing dictionary: %+v %v", out, err)
	}
	sentinel := errors.New("database failure")
	d.err = sentinel
	if _, err := NewReadDictionariesLogic(ctx, sc).ReadDictionaries(&system.ReadDictionariesRequest{Codes: []string{"empty"}}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	d.err = nil
	i.err = sentinel
	if _, err := NewReadDictionariesLogic(ctx, sc).ReadDictionaries(&system.ReadDictionariesRequest{Codes: []string{"empty"}}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func TestDictionaryManagementReadsAndErrors(t *testing.T) {
	ctx := context.Background()
	sc, d, i, c := dictionaryFixture()
	d.items = []model.Dictionary{*d.item}
	i.items = []model.DictionaryItem{*i.item}
	if r, e := NewListDictionariesLogic(ctx, sc).ListDictionaries(nil); e != nil || len(r.Items) != 1 {
		t.Fatal(r, e)
	}
	if r, e := NewListDictionaryItemsLogic(ctx, sc).ListDictionaryItems(&system.ListDictionaryItemsRequest{DictionaryId: 7}); e != nil || len(r.Items) != 1 {
		t.Fatal(r, e)
	}
	if r, e := NewGetDictionaryLogic(ctx, sc).GetDictionary(&system.GetDictionaryRequest{Id: 7}); e != nil || r.Dictionary.Code != "source" {
		t.Fatal(r, e)
	}
	if r, e := NewGetDictionaryItemLogic(ctx, sc).GetDictionaryItem(&system.GetDictionaryItemRequest{Id: 8}); e != nil || r.Item.Value != "web" {
		t.Fatal(r, e)
	}
	if _, e := NewClearDictionaryCacheLogic(ctx, sc).ClearDictionaryCache(nil); e != nil || !c.cleared {
		t.Fatal(e)
	}
	c.clearErr = errors.New("redis down")
	if _, e := NewClearDictionaryCacheLogic(ctx, sc).ClearDictionaryCache(nil); e == nil {
		t.Fatal("clear failure swallowed")
	}
	d.err = repository.ErrDictionaryNotFound
	i.err = repository.ErrDictionaryItemNotFound
	if _, e := NewListDictionariesLogic(ctx, sc).ListDictionaries(nil); e == nil {
		t.Fatal("list failure swallowed")
	}
	if _, e := NewListDictionaryItemsLogic(ctx, sc).ListDictionaryItems(&system.ListDictionaryItemsRequest{DictionaryId: 7}); e == nil {
		t.Fatal("missing parent")
	}
	if _, e := NewGetDictionaryLogic(ctx, sc).GetDictionary(&system.GetDictionaryRequest{Id: 7}); e == nil {
		t.Fatal("missing dictionary")
	}
	if _, e := NewGetDictionaryItemLogic(ctx, sc).GetDictionaryItem(&system.GetDictionaryItemRequest{Id: 8}); e == nil {
		t.Fatal("missing item")
	}
	d.err = nil
	if _, e := NewListDictionaryItemsLogic(ctx, sc).ListDictionaryItems(&system.ListDictionaryItemsRequest{DictionaryId: 7}); e == nil {
		t.Fatal("list items failure")
	}
	for _, e := range []error{repository.ErrDictionaryNotFound, repository.ErrDictionaryCodeExists, repository.ErrDictionaryHasItems, repository.ErrDictionaryItemNotFound, repository.ErrDictionaryValueExists} {
		if dictionaryError(e) == nil {
			t.Fatal(e)
		}
	}
	if dictionaryError(nil) != nil {
		t.Fatal("nil error changed")
	}
	v := model.Dictionary{Base: model.Base{CreatedAt: time.Date(2026, 9, 28, 8, 0, 0, 0, time.FixedZone("CST", 8*3600))}}
	if toDictionaryInfo(v).CreatedAt != "2026-09-28T00:00:00Z" {
		t.Fatal("timestamp not normalized")
	}
}
