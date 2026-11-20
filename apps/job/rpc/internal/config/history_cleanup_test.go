package config

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
)

func TestHistoryCleanupDeploymentDefaultsAndOmission(t *testing.T) {
	want := HistoryCleanupConf{RetentionDays: 7, Interval: 4 * time.Hour}
	var defaults HistoryCleanupConf
	if err := conf.LoadFromYamlBytes([]byte("{}"), &defaults); err != nil {
		t.Fatal(err)
	}
	if defaults != want || defaults.Validate() != nil {
		t.Fatalf("unexpected history cleanup defaults: %+v", defaults)
	}
	content, err := os.ReadFile("../../etc/job-rpc.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	example := strings.ReplaceAll(string(content), "\r\n", "\n")
	start, end := strings.Index(example, "HistoryCleanup:\n"), strings.Index(example, "\nDispatch:\n")
	if start < 0 || end <= start {
		t.Fatal("example history cleanup block was not found")
	}
	for _, input := range []string{example, example[:start] + example[end:]} {
		var value Config
		if err := conf.LoadFromYamlBytes([]byte(input), &value); err != nil {
			t.Fatal(err)
		}
		if value.HistoryCleanup != want {
			t.Fatalf("omitted or example history cleanup defaults: %+v", value.HistoryCleanup)
		}
		if err := value.PrepareExecution(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHistoryCleanupRejectsNonpositiveAndOverflowDurations(t *testing.T) {
	maxDays := int((1<<63 - 1) / (24 * time.Hour))
	for _, input := range []string{
		"RetentionDays: 0", "RetentionDays: -1", fmt.Sprintf("RetentionDays: %d", maxDays+1),
		"Interval: 0s", "Interval: -1s",
	} {
		t.Run(input, func(t *testing.T) {
			var value HistoryCleanupConf
			err := conf.LoadFromYamlBytes([]byte(input), &value)
			if err == nil {
				err = value.Validate()
			}
			if err == nil {
				t.Fatal("invalid history cleanup settings were accepted or replaced with defaults")
			}
		})
	}
	for _, days := range []int{1, maxDays} {
		value := HistoryCleanupConf{RetentionDays: days, Interval: time.Nanosecond}
		if err := value.Validate(); err != nil || time.Duration(days)*24*time.Hour <= 0 {
			t.Fatalf("valid history retention rejected: days=%d error=%v", days, err)
		}
	}
	for _, value := range []HistoryCleanupConf{{}, {RetentionDays: 7}, {Interval: time.Hour}, {RetentionDays: maxDays + 1, Interval: time.Hour}} {
		c := validExecutionConfig()
		c.HistoryCleanup = value
		if c.PrepareExecution() == nil {
			t.Fatalf("invalid history cleanup passed host validation: %+v", value)
		}
	}
}
