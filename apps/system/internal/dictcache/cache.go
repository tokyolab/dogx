// Package dictcache stores business dictionary options, never management DTOs.
package dictcache

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/zeromicro/go-zero/core/stores/redis"
)

const TTLSeconds = 24 * 60 * 60

// A shared hash tag lets content and its cleanup index be written atomically on Redis Cluster.
const keyPrefix = "dogx:system:{dict}:"

type Item struct {
	Label  string `json:"label"`
	Value  string `json:"value"`
	Status int32  `json:"status"`
	Sort   int32  `json:"sort"`
}
type Entry struct {
	Status   int32  `json:"status"`
	IsPublic bool   `json:"isPublic"`
	Items    []Item `json:"items"`
}
type Store interface {
	Read(context.Context, []string) (map[string]Entry, error)
	Put(context.Context, string, Entry) error
	Invalidate(context.Context, string) error
	Clear(context.Context) error
}
type client interface {
	MgetCtx(context.Context, ...string) ([]string, error)
	EvalCtx(context.Context, string, []string, ...any) (any, error)
	SscanCtx(context.Context, string, uint64, string, int64) ([]string, uint64, error)
}
type Cache struct {
	client client
	prefix string
}

func (c *Cache) key(code string) string { return c.prefix + code }
func (c *Cache) index() string          { return c.key("_index") }

func New(client *redis.Redis) *Cache { return &Cache{client: client, prefix: keyPrefix} }

// SET and index insertion must succeed together, otherwise "clear all" could miss an orphan key.
const putScript = `
redis.call('SADD', KEYS[1], KEYS[2])
redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[2])
return 1
`
const removeScript = `
for i = 2, #KEYS do
 redis.call('DEL', KEYS[i])
 redis.call('SREM', KEYS[1], KEYS[i])
end
return 1
`

func (c *Cache) Read(ctx context.Context, codes []string) (map[string]Entry, error) {
	result := make(map[string]Entry, len(codes))
	if len(codes) == 0 {
		return result, nil
	}
	keys := make([]string, len(codes))
	for i, code := range codes {
		keys[i] = c.key(code)
	}
	values, err := c.client.MgetCtx(ctx, keys...)
	if err != nil {
		return nil, err
	}
	if len(values) != len(codes) {
		return nil, errors.New("invalid dictionary cache response")
	}
	for i, value := range values {
		if value == "" {
			continue
		}
		var entry Entry
		// Malformed entries are treated as misses and replaced from the database.
		if err := json.Unmarshal([]byte(value), &entry); err != nil || entry.Items == nil {
			continue
		}
		result[codes[i]] = entry
	}
	return result, nil
}
func (c *Cache) Put(ctx context.Context, code string, entry Entry) error {
	if entry.Items == nil {
		entry.Items = []Item{}
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = c.client.EvalCtx(ctx, putScript, []string{c.index(), c.key(code)}, string(payload), TTLSeconds)
	return err
}
func (c *Cache) Invalidate(ctx context.Context, code string) error {
	_, err := c.client.EvalCtx(ctx, removeScript, []string{c.index(), c.key(code)})
	return err
}
func (c *Cache) Clear(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := c.client.SscanCtx(ctx, c.index(), cursor, "", 200)
		if err != nil {
			return err
		}
		// Only keys owned by this feature can be deleted, even if the index was manually corrupted.
		owned := []string{c.index()}
		for _, key := range keys {
			if len(key) > len(c.prefix) && key[:len(c.prefix)] == c.prefix && key != c.index() {
				owned = append(owned, key)
			}
		}
		for start := 1; start < len(owned); start += 200 {
			end := min(start+200, len(owned))
			batch := append([]string{c.index()}, owned[start:end]...)
			if _, err = c.client.EvalCtx(ctx, removeScript, batch); err != nil {
				return err
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}
