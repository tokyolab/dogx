package repository

import "strings"

func containsLikePattern(value string) string {
	escaped := strings.NewReplacer(
		"!", "!!",
		"%", "!%",
		"_", "!_",
	).Replace(value)
	return "%" + escaped + "%"
}
