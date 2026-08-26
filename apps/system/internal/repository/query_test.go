package repository

import "testing"

func TestContainsLikePatternEscapesWildcards(t *testing.T) {
	if got, want := containsLikePattern("a!b%c_d"), "%a!!b!%c!_d%"; got != want {
		t.Fatalf("LIKE pattern = %q, want %q", got, want)
	}
}
