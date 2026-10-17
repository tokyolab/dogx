package repository

import "testing"

func TestRepositoryConstructorsRejectNilDatabase(t *testing.T) {
	if repo, err := NewJobDefinitionRepository(nil); repo != nil || err == nil {
		t.Fatalf("job definition repository=%v error=%v", repo, err)
	}
	if repo, err := NewJobInstanceRepository(nil); repo != nil || err == nil {
		t.Fatalf("job instance repository=%v error=%v", repo, err)
	}
	if repo, err := NewExecutorGroupRepository(nil); repo != nil || err == nil {
		t.Fatalf("executor group repository=%v error=%v", repo, err)
	}
}
