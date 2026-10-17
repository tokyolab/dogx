// Package schedule defines the Cron grammar shared by validation, previews,
// persisted progress, and leader timers. Expressions use the scheduler process's
// time.Local; all scheduler replicas must use the same deployment timezone.
package schedule

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/robfig/cron/v3"
)

var parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// Parse parses six-field Cron expressions (second, minute, hour, day of month,
// month, day of week) after trimming surrounding whitespace, up to 128 characters.
// Descriptors such as @daily, years, and embedded timezones are unsupported;
// month and weekday syntax follows robfig. The returned schedule always uses
// time.Local rather than the input timestamp's Location.
//
// Parsing validates syntax, not the existence of a future occurrence. Callers
// of the returned schedule's Next must handle the zero time returned when no
// occurrence lies within its search range, or use this package's Next instead.
// robfig recognizes timezone prefixes independently of its parser field options,
// so those options alone cannot enforce this grammar.
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

// Next returns the first scheduled occurrence strictly later than after, interpreting
// the expression in the scheduler process's timezone. Invalid expressions or
// no occurrence within robfig's search range return zero time and an error.
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

// Preview returns up to five scheduled occurrences strictly later than after, in
// increasing order. Invalid syntax or no first occurrence returns nil and an
// error. If a later search reaches its limit, already found occurrences are
// returned with a nil error; missing occurrences are not synthesized.
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
