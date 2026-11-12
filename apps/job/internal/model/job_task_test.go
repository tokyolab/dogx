package model

import (
	"bytes"
	"testing"
)

func TestTaskDataPreservesJSONAndNullability(t *testing.T) {
	for _, raw := range []TaskData{nil, []byte("null"), []byte("true"), []byte("9007199254740993"), []byte(`{"id":9223372036854775807}`), []byte(`[1,true,"text",[null]]`), []byte(`"scalar"`)} {
		value, err := raw.Value()
		if err != nil {
			t.Fatal(err)
		}
		if raw == nil && value != nil {
			t.Fatalf("SQL NULL encoded as %T", value)
		}
		var decoded TaskData
		if err := decoded.Scan(value); err != nil || !bytes.Equal(decoded, raw) || (decoded == nil) != (raw == nil) {
			t.Fatalf("round trip changed JSON: got=%s want=%s err=%v", decoded, raw, err)
		}
	}
}

func TestTaskDataOwnsScannedBytesAndRejectsInvalidValues(t *testing.T) {
	input := []byte(`{"id":9223372036854775807}`)
	var value TaskData
	if err := value.Scan(input); err != nil {
		t.Fatal(err)
	}
	input[0] = '['
	if string(value) != `{"id":9223372036854775807}` {
		t.Fatal("Scan retained driver-owned bytes")
	}
	for _, invalid := range []any{1, []byte{}, "", "{broken"} {
		before := append(TaskData(nil), value...)
		if err := value.Scan(invalid); err == nil || !bytes.Equal(before, value) {
			t.Fatalf("invalid Scan changed receiver or succeeded: input=%T err=%v", invalid, err)
		}
	}
	for _, invalid := range []TaskData{{}, []byte("{broken")} {
		if _, err := invalid.Value(); err == nil {
			t.Fatal("invalid JSON accepted for persistence")
		}
	}
	if err := value.Scan(nil); err != nil || value != nil {
		t.Fatalf("SQL NULL did not clear receiver: %v", err)
	}
}
