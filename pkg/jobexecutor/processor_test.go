package jobexecutor

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestResultValidatePreservesOptionalRawJSON(t *testing.T) {
	cases := []struct {
		name string
		data json.RawMessage
		ok   bool
	}{
		{name: "absent", ok: true},
		{name: "explicit null", data: json.RawMessage(`null`), ok: true},
		{name: "large integer", data: json.RawMessage(`9007199254740993`), ok: true},
		{name: "scalar", data: json.RawMessage(`"value"`), ok: true},
		{name: "array", data: json.RawMessage(`[1,true,null]`), ok: true},
		{name: "empty provided bytes", data: json.RawMessage{}, ok: false},
		{name: "malformed", data: json.RawMessage(`{"x":`), ok: false},
		{name: "multiple values", data: json.RawMessage(`1 2`), ok: false},
		{name: "JSONB validation is separate", data: json.RawMessage(`"\u0000"`), ok: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			before := bytes.Clone(test.data)
			err := (Result{Data: test.data}).Validate()
			if (err == nil) != test.ok {
				t.Fatalf("Validate() error = %v, want valid %v", err, test.ok)
			}
			if !bytes.Equal(before, test.data) {
				t.Fatal("Validate changed the original JSON encoding")
			}
		})
	}
}

func TestPrepareResultRequiresExplicitItems(t *testing.T) {
	cases := []struct {
		name  string
		items []json.RawMessage
		ok    bool
	}{
		{name: "missing", ok: false},
		{name: "explicit zero shards", items: []json.RawMessage{}, ok: true},
		{name: "independent JSON values", items: []json.RawMessage{json.RawMessage(`9007199254740993`), json.RawMessage(`{"from":1,"to":3}`), json.RawMessage(`null`)}, ok: true},
		{name: "missing item", items: []json.RawMessage{nil}, ok: false},
		{name: "invalid later item", items: []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`no-json`)}, ok: false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := (PrepareResult{Items: test.items}).Validate()
			if (err == nil) != test.ok {
				t.Fatalf("Validate() error = %v, want valid %v", err, test.ok)
			}
		})
	}
}
