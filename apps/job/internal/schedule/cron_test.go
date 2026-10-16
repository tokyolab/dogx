package schedule

import (
	"strings"
	"testing"
	"time"
	_ "time/tzdata"
)

func TestGrammarAndNext(t *testing.T) {
	at := time.Date(2026, 10, 16, 8, 0, 0, 123, time.Local)
	for _, expr := range []string{"* * * * *", "0 * * * * * 2026", "@daily", "@every 1s", "TZ=UTC 0 * * * * *", "CRON_TZ=UTC 0 * * * * *", "TZ=UTC 0 * * * *", "CRON_TZ=UTC 0 * * * *", "0 0 0 * * 7", "0 0 0 L * *", "0 0 0 * * MON#2", "0 0 0 1W * *", "0 0 0 31 FEB *", "0 0 0 * * *\x00", string([]byte{255}), "0" + strings.Repeat(" ", 128) + "0 0 * * *"} {
		t.Run(expr, func(t *testing.T) {
			if _, err := Next(expr, at); err == nil {
				t.Fatalf("accepted invalid or unsatisfiable expression %q", expr)
			}
		})
	}
	for _, expr := range []string{"* * * * * *", "0 */5 * * * *", "0 0 9 * JAN-DEC MON-FRI", "0 0 9 ? * MON", " 0 0 9 * * * "} {
		t.Run(expr, func(t *testing.T) {
			next, err := Next(expr, at)
			if err != nil || !next.After(at) || next.Nanosecond() != 0 {
				t.Fatalf("next=%s err=%v", next, err)
			}
		})
	}
}

func TestPreviewKeepsValidOccurrencesWhenLaterSearchRangeEnds(t *testing.T) {
	// 2100 is not a leap year. After the 2096 occurrence robfig's five-year
	// search horizon cannot reach 2104, so Preview must retain its first result.
	after := time.Date(2092, 3, 1, 0, 0, 0, 0, time.Local)
	times, err := Preview("0 0 0 29 FEB *", after)
	want := time.Date(2096, 2, 29, 0, 0, 0, 0, time.Local)
	if err != nil || len(times) != 1 || !times[0].Equal(want) {
		t.Fatalf("partial preview=%v error=%v", times, err)
	}
}

func TestDeploymentDSTSkipsNonexistentLocalTime(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	previous := time.Local
	time.Local = location
	t.Cleanup(func() { time.Local = previous })
	after := time.Date(2026, 3, 7, 2, 30, 0, 0, location)
	want := time.Date(2026, 3, 9, 2, 30, 0, 0, location)
	next, err := Next("0 30 2 * * *", after.UTC())
	if err != nil || !next.Equal(want) {
		t.Fatalf("nonexistent local 02:30 was compensated or timezone was lost: next=%s error=%v", next, err)
	}
}

func TestProcessTimezoneIgnoresDatabaseDisplayZone(t *testing.T) {
	previous := time.Local
	t.Cleanup(func() { time.Local = previous })
	time.Local = time.FixedZone("deployment", 8*60*60)
	at := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	want := time.Date(2026, 10, 16, 1, 0, 0, 0, time.UTC)
	s, err := Parse("0 0 9 * * *")
	if err != nil {
		t.Fatal(err)
	}
	if next := s.Next(at); !next.Equal(want) {
		t.Fatalf("schedule Next=%s, want %s", next, want)
	}
	if next, err := Next("0 0 9 * * *", at); err != nil || !next.Equal(want) {
		t.Fatalf("Next=%s err=%v", next, err)
	}
	times, err := Preview("0 0 9 * * *", at)
	if err != nil || len(times) != 5 || !times[0].Equal(want) {
		t.Fatalf("preview=%v err=%v", times, err)
	}
	for i := 1; i < len(times); i++ {
		if times[i].Sub(times[i-1]) != 24*time.Hour {
			t.Fatalf("unexpected preview interval: %v", times)
		}
	}
}

func TestCalendarOrSemanticsAndStrictNext(t *testing.T) {
	at := time.Date(2026, 10, 16, 9, 0, 0, 0, time.Local)
	next, err := Next("0 0 9 1 * MON", at)
	want := time.Date(2026, 10, 19, 9, 0, 0, 0, time.Local)
	if err != nil || !next.Equal(want) {
		t.Fatalf("day/month OR semantics next=%s err=%v", next, err)
	}
	next, err = Next("* * * * * *", at)
	if err != nil || !next.Equal(at.Add(time.Second)) {
		t.Fatalf("strict next=%s err=%v", next, err)
	}
	if _, err := Preview("0 0 0 31 FEB *", at); err == nil {
		t.Fatal("unsatisfiable preview accepted")
	}
	if _, err := Preview("@daily", at); err == nil {
		t.Fatal("descriptor preview accepted")
	}
}
