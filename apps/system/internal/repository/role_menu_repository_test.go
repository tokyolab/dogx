package repository

import (
	"errors"
	"slices"
	"testing"

	"github.com/tokyolab/dogx/apps/system/internal/model"
)

func TestNormalizeRoleMenuIDs(t *testing.T) {
	one, two := int64(1), int64(2)
	nodes := []model.Menu{{Base: model.Base{ID: 1}}, {Base: model.Base{ID: 2}, ParentID: &one}, {Base: model.Base{ID: 3}, ParentID: &two}, {Base: model.Base{ID: 4}, ParentID: &two}}
	for _, tc := range []struct{ in, want []int64 }{{nil, []int64{}}, {[]int64{3, 3}, []int64{1, 2, 3}}, {[]int64{1}, []int64{1}}} {
		got, err := normalizeRoleMenuIDs(nodes, tc.in)
		if err != nil || !slices.Equal(got, tc.want) {
			t.Fatalf("got=%v err=%v", got, err)
		}
	}
	if _, err := normalizeRoleMenuIDs(nodes, []int64{99}); !errors.Is(err, ErrRoleMenuUnavailable) {
		t.Fatal(err)
	}
	nodes[0].ParentID = &two
	if _, err := normalizeRoleMenuIDs(nodes, []int64{3}); !errors.Is(err, ErrRoleMenuUnavailable) {
		t.Fatal(err)
	}
	if _, err := NewRoleMenuRepository(nil); err == nil {
		t.Fatal("nil database accepted")
	}
}
