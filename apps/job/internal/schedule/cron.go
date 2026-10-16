// Package schedule owns the Cron grammar and process-timezone interpretation
// shared by validation, previews, persistence, and the leader's timer.
package schedule

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/robfig/cron/v3"
)

var parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// Parse deliberately excludes descriptors and embedded timezones, even though
// robfig accepts timezone prefixes independently of its parser options.
func Parse(expr string) (cron.Schedule, error) {
	expr = strings.TrimSpace(expr)
	if !utf8.ValidString(expr) || strings.ContainsRune(expr, 0) || utf8.RuneCountInString(expr) > 128 || len(strings.Fields(expr)) != 6 {
		return nil, errors.New("cron must contain six fields and at most 128 characters")
	}
	if strings.HasPrefix(expr, "TZ=") || strings.HasPrefix(expr, "CRON_TZ=") {
		return nil, errors.New("cron timezone prefixes are not supported")
	}
	s, err := parser.Parse(expr)
	if err != nil {
		return nil, err
	}
	return localSchedule{s}, nil
}

type localSchedule struct{ cron.Schedule }

func (s localSchedule) Next(after time.Time) time.Time {
	// PostgreSQL may return UTC. robfig's Local schedules otherwise inherit the
	// input timestamp's location, making persistence disagree with live callbacks.
	return s.Schedule.Next(after.In(time.Local))
}

func Next(expr string, after time.Time) (time.Time, error) {
	s, err := Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	next := s.Next(after)
	if next.IsZero() || !next.After(after) {
		return time.Time{}, errors.New("cron has no next occurrence within the supported search range")
	}
	return next, nil
}

func Preview(expr string, after time.Time) ([]time.Time, error) {
	s, err := Parse(expr)
	if err != nil {
		return nil, err
	}
	times := make([]time.Time, 0, 5)
	for range 5 {
		next := s.Next(after)
		if next.IsZero() || !next.After(after) {
			if len(times) == 0 {
				return nil, errors.New("cron has no next occurrence within the supported search range")
			}
			break
		}
		times = append(times, next)
		after = next
	}
	return times, nil
}
