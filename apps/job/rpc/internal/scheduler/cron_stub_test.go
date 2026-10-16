package scheduler

import (
	"context"
	"time"

	"github.com/robfig/cron/v3"
)

type cronStub struct {
	entries   map[cron.EntryID]cron.Entry
	nextID    cron.EntryID
	started   bool
	starts    int
	barriers  int
	onBarrier func()
	onAdd     func()
}

func (c *cronStub) Schedule(s cron.Schedule, j cron.Job) cron.EntryID {
	if c.entries == nil {
		c.entries = make(map[cron.EntryID]cron.Entry)
	}
	c.nextID++
	c.entries[c.nextID] = cron.Entry{ID: c.nextID, Schedule: s, Job: j}
	if c.onAdd != nil {
		c.onAdd()
	}
	return c.nextID
}

func (c *cronStub) Remove(id cron.EntryID) { delete(c.entries, id) }
func (c *cronStub) Start()                 { c.started = true; c.starts++ }
func (c *cronStub) Entries() []cron.Entry {
	c.barriers++
	if c.onBarrier != nil {
		c.onBarrier()
	}
	entries := make([]cron.Entry, 0, len(c.entries))
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(time.Now())
		entries = append(entries, entry)
	}
	return entries
}

func (c *cronStub) Stop() context.Context {
	c.started = false
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
