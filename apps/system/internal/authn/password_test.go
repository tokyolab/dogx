package authn

import (
	"errors"
	"strings"
	"testing"
)

func TestArgon2idHashAndVerify(t *testing.T) {
	hasher := NewArgon2id()

	encoded, err := hasher.Hash("correct horse battery staple")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	if !strings.HasPrefix(encoded, "$argon2id$v=19$") {
		t.Fatalf("unexpected password hash format: %s", encoded)
	}
	if err := hasher.Verify(encoded, "correct horse battery staple"); err != nil {
		t.Fatalf("verify correct password: %v", err)
	}
	if err := hasher.Verify(encoded, "wrong password"); !errors.Is(err, ErrPasswordMismatch) {
		t.Fatalf("expected password mismatch, got: %v", err)
	}
}

func TestArgon2idRejectsEmptyPasswordAndMalformedHashes(t *testing.T) {
	hasher := NewArgon2id()
	if _, err := hasher.Hash(""); err == nil {
		t.Fatal("expected empty password to be rejected")
	}

	invalid := []string{
		"not-a-password-hash",
		"$argon2id$v=18$m=65536,t=3,p=2$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaA",
		"$argon2id$v=19$m=1,t=3,p=2$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaA",
		"$argon2id$v=19$m=65536,t=0,p=2$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaA",
		"$argon2id$v=19$m=65536,t=3,p=0$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaA",
		"$argon2id$v=19$m=65536,t=3,p=2$bad*$aGFzaGhhc2hoYXNoaGFzaA",
		"$argon2id$v=19$m=65536,t=3,p=2$c2FsdHNhbHQ$bad*",
	}
	for _, encoded := range invalid {
		if err := hasher.Verify(encoded, "password"); err == nil {
			t.Fatalf("expected malformed hash to be rejected: %s", encoded)
		}
	}
}
