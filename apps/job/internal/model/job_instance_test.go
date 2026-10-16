package model

import (
	"encoding/json"
	"testing"
)

func TestExecutionConfigPreservesAllMembers(t *testing.T) {
	config := ExecutionConfig{SchemaVersion: 1, ProcessorName: "p", ExecutionMode: ExecutionModeNormal, RetryIntervalSeconds: 10, TimeoutSeconds: 900}
	value, err := config.Value()
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(value.(string)), &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 8 || fields["params"] != "" || fields["post_enabled"] != false || fields["max_retries"] != float64(0) {
		t.Fatalf("snapshot=%s", value)
	}
	for _, raw := range []any{value, []byte(value.(string))} {
		var loaded ExecutionConfig
		if err := loaded.Scan(raw); err != nil || loaded != config {
			t.Fatalf("loaded=%+v err=%v", loaded, err)
		}
	}
	var loaded ExecutionConfig
	if err := loaded.Scan(nil); err == nil {
		t.Fatal("accepted SQL NULL snapshot")
	}
}
