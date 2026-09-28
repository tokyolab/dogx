package dictcache

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestReadHandlesMissEmptyAndCorruptEntries(t *testing.T) {
	r := &redisClientStub{values: []string{"", `{"status":1,"items":[]}`, `{"status":0,"isPublic":true,"items":[{"label":"旧","value":"old","status":0,"sort":2}]}`, "{", "null"}}
	c := &Cache{client: r, prefix: keyPrefix}
	ctx := context.Background()
	result, err := c.Read(ctx, []string{"missing", "empty", "history", "broken", "null"})
	if err != nil || len(result) != 2 || result["empty"].Items == nil || result["history"].Items[0].Status != 0 {
		t.Fatal(result, err)
	}
	if v, e := c.Read(ctx, nil); e != nil || len(v) != 0 {
		t.Fatal(v, e)
	}
	r.values = nil
	if _, e := c.Read(ctx, []string{"source"}); e == nil {
		t.Fatal("invalid mget length")
	}
	sentinel := errors.New("redis unavailable")
	r.readErr = sentinel
	if _, e := c.Read(ctx, []string{"source"}); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
}
func TestAtomicCacheWriteAndInvalidation(t *testing.T) {
	r := &redisClientStub{}
	c := &Cache{client: r, prefix: keyPrefix}
	ctx := context.Background()
	if err := c.Put(ctx, "source", Entry{Status: 1}); err != nil {
		t.Fatal(err)
	}
	if r.script != putScript || len(r.keys) != 2 || r.keys[0] != c.index() || r.keys[1] != c.key("source") || r.args[1] != 86400 || !strings.Contains(r.args[0].(string), `"items":[]`) {
		t.Fatalf("wrong atomic write: %+v", r)
	}
	if err := c.Invalidate(ctx, "source"); err != nil || r.script != removeScript {
		t.Fatal(err)
	}
	sentinel := errors.New("redis down")
	r.evalErr = sentinel
	if err := c.Put(ctx, "source", Entry{}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if err := c.Invalidate(ctx, "source"); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}
func TestClearBoundedIndexPagesAndFailures(t *testing.T) {
	r := &redisClientStub{pages: [][]string{{keyPrefix + "orphan", "other:secret", keyPrefix + "_index"}, {keyPrefix + "empty"}}}
	c := &Cache{client: r, prefix: keyPrefix}
	ctx := context.Background()
	if err := c.Clear(ctx); err != nil || r.scanned != 2 || r.evals != 2 || len(r.keys) != 2 || r.keys[1] != c.key("empty") {
		t.Fatal(r, err)
	}
	keys := make([]string, 450)
	for i := range keys {
		keys[i] = c.key("source")
	}
	r.pages = [][]string{keys}
	r.evals = 0
	r.scanned = 0
	if err := c.Clear(ctx); err != nil || r.evals != 3 || len(r.keys) != 51 {
		t.Fatal(r, err)
	}
	sentinel := errors.New("redis down")
	r.scanErr = sentinel
	if err := c.Clear(ctx); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	r.scanErr = nil
	r.evalErr = sentinel
	if err := c.Clear(ctx); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
}
