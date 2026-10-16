package repository

import "testing"

func TestLiteralContainsEscapesWildcardCharacters(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", "%%"}, {"plain", "%plain%"}, {"50%_done!", "%50!%!_done!!%"},
		{"!!%_", "%!!!!!%!_%"}, {"中文", "%中文%"},
	} {
		if got := literalContains(tc.input); got != tc.want {
			t.Errorf("literalContains(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}
