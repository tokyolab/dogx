package svc

import "testing"

func TestServiceContextCloseWithoutDependencies(t *testing.T) {
	if err := (&ServiceContext{}).Close(); err != nil {
		t.Fatalf("close empty service context: %v", err)
	}
}
