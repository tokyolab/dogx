//go:build integration && e2e && load

package e2e

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// jobHistoryCleanupPass joins a real candidate-query cutoff with the host's
// completion log. DatabaseClockAt is the database clock used by the pass, not
// an inferred timer deadline; pool acquisition before that read is not measured.
type jobHistoryCleanupPass struct {
	Process                                         string
	Cutoff, DatabaseClockAt, CompletedAt            time.Time
	InstancesDeleted, TasksDeleted, Failed, Skipped int64
	DatabaseClockToCompletionMS                     float64
}

type jobHistoryOverlap struct {
	Start, End                                                             time.Time
	ScheduledInstances, ActiveBusinessCalls, OrdinarySamples, ShardSamples int
	OrdinaryStart, ShardStart, ResultRecordDifference, ParentVisible       jobLoadDistribution
}

var historyParameter = regexp.MustCompile(`\$1 = '([^']+)'`)

func readHistoryCleanupPasses(t testing.TB, processes []*runningProcess, names map[string]string, retention int) []jobHistoryCleanupPass {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(os.Getenv("DOGX_HISTORY_PG_LOG_DIR"), "*.csv"))
	if err != nil || len(files) == 0 {
		t.Fatal("PostgreSQL CSV history trace is missing")
	}
	byProcess := make(map[string][]time.Time)
	seen := make(map[string]bool)
	for _, path := range files {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1
		for {
			record, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				// The logging collector may still be appending its final record.
				// All matched, completed passes must independently have both ends.
				if strings.Contains(err.Error(), "extraneous or missing") || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				_ = file.Close()
				t.Fatalf("read PostgreSQL CSV history trace: %v", err)
			}
			if len(record) < 23 {
				continue
			}
			name, ok := names[record[22]]
			if !ok || !strings.HasPrefix(name, "history-job-") {
				continue
			}
			statement := record[13]
			// Extended-protocol Parse and Bind records do not prove execution and
			// may omit values. Only Execute records carry the real pass cutoff.
			if !strings.Contains(statement, " execute ") {
				continue
			}
			if !strings.Contains(statement, `SELECT "id","finished_at" FROM "job_instance"`) || !strings.Contains(statement, "status IN (3, 4, 5)") {
				continue
			}
			match := historyParameter.FindStringSubmatch(record[14])
			if len(match) != 2 {
				_ = file.Close()
				t.Fatal("candidate trace is missing its original cutoff parameter")
			}
			cutoff, err := historyTraceTime(match[1])
			if err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			key := name + "/" + cutoff.Format(time.RFC3339Nano)
			if !seen[key] {
				byProcess[name] = append(byProcess[name], cutoff)
				seen[key] = true
			}
		}
		_ = file.Close()
	}
	var passes []jobHistoryCleanupPass
	for _, p := range processes {
		cutoffs := byProcess[p.name]
		sort.Slice(cutoffs, func(i, j int) bool { return cutoffs[i].Before(cutoffs[j]) })
		var completions []jobHistoryCleanupPass
		for _, line := range strings.Split(p.logs.String(), "\n") {
			var value struct {
				At      string `json:"@timestamp"`
				Content string `json:"content"`
				Deleted int64  `json:"instances_deleted"`
				Tasks   int64  `json:"tasks_deleted"`
				Failed  int64  `json:"instances_failed"`
				Skipped int64  `json:"instances_skipped"`
			}
			if json.Unmarshal([]byte(line), &value) != nil || value.Content != "job history cleanup completed" && value.Content != "job history cleanup failed" {
				continue
			}
			at, err := historyTraceTime(value.At)
			if err != nil {
				t.Fatal(err)
			}
			completions = append(completions, jobHistoryCleanupPass{Process: p.name, CompletedAt: at, InstancesDeleted: value.Deleted, TasksDeleted: value.Tasks, Failed: value.Failed, Skipped: value.Skipped})
		}
		if len(cutoffs) != len(completions) {
			t.Fatalf("incomplete cleanup trace for %s: cutoff_count=%d completion_count=%d", p.name, len(cutoffs), len(completions))
		}
		for i, cutoff := range cutoffs {
			pass := completions[i]
			pass.Cutoff = cutoff
			pass.DatabaseClockAt = cutoff.Add(time.Duration(retention) * 24 * time.Hour)
			pass.DatabaseClockToCompletionMS = pass.CompletedAt.Sub(pass.DatabaseClockAt).Seconds() * 1000
			if pass.DatabaseClockToCompletionMS < -2 {
				t.Fatal("cleanup trace timestamps are inconsistent")
			}
			passes = append(passes, pass)
		}
	}
	sort.Slice(passes, func(i, j int) bool { return passes[i].DatabaseClockAt.Before(passes[j].DatabaseClockAt) })
	if len(passes) == 0 {
		t.Fatal("no actual Leader cleanup trace was observed")
	}
	return passes
}

func historyTraceTime(value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999-07", "2006-01-02 15:04:05.999999999 MST"} {
		if at, err := time.Parse(layout, value); err == nil {
			return at.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp in isolated history trace")
}

func measureHistoryOverlap(t testing.TB, report *jobHistoryLoadReport) {
	var parents, tasks int64
	for _, pass := range report.Cleanup {
		parents += pass.InstancesDeleted
		tasks += pass.TasksDeleted
		if pass.InstancesDeleted == 0 {
			continue
		}
		if pass.DatabaseClockAt.Before(report.FirstDueAt) || pass.CompletedAt.After(report.WindowEnd) {
			t.Fatal("history was deleted outside the approved continuous window")
		}
		overlap := jobHistoryOverlap{Start: pass.DatabaseClockAt, End: pass.CompletedAt}
		var ordinary, shards, result, visible []float64
		for _, e := range report.Events {
			if !e.Started.After(overlap.End) && !e.BusinessFinished.Before(overlap.Start) {
				overlap.ActiveBusinessCalls++
			}
			// Select by scheduled time, retaining calls delayed until after cleanup.
			// Selecting only early business starts would hide the worst tail.
			if e.Due.Before(overlap.Start) || e.Due.After(overlap.End) {
				continue
			}
			if e.Kind == 1 {
				ordinary = append(ordinary, e.Started.Sub(e.Due).Seconds()*1000)
			}
			if e.Kind == 3 {
				shards = append(shards, e.Started.Sub(e.Due).Seconds()*1000)
			}
			result = append(result, e.Saved.Sub(e.BusinessFinished).Seconds()*1000)
		}
		for _, v := range report.Visible {
			if !v.Due.Before(overlap.Start) && !v.Due.After(overlap.End) {
				visible = append(visible, v.Milliseconds)
			}
		}
		overlap.ScheduledInstances = len(visible)
		overlap.OrdinarySamples = len(ordinary)
		overlap.ShardSamples = len(shards)
		overlap.OrdinaryStart = jobShardedLoadDistribution(ordinary)
		overlap.ShardStart = jobShardedLoadDistribution(shards)
		overlap.ResultRecordDifference = jobShardedLoadDistribution(result)
		overlap.ParentVisible = jobShardedLoadDistribution(visible)
		report.Overlaps = append(report.Overlaps, overlap)
		if overlap.ActiveBusinessCalls == 0 || len(ordinary) == 0 || len(shards) == 0 {
			t.Fatal("cleanup did not overlap enough actual ordinary and sharded business to verify its impact")
		}
		if overlap.OrdinaryStart.P99 > 10000 || overlap.ShardStart.P95 > 15000 || overlap.ShardStart.P99 > 20000 || overlap.ParentVisible.Max > 30000 {
			t.Fatal("cleanup overlap exceeded an approved online latency threshold")
		}
	}
	if parents != 20000 || tasks != 230000 {
		t.Fatalf("cleanup logs report parents=%d tasks=%d, want 20000/230000", parents, tasks)
	}
}
