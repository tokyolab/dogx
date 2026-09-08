package authn

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestBcryptHashAndVerify(t *testing.T) {
	hasher := NewBcrypt()
	password := "correct horse battery staple"
	encoded, err := hasher.Hash(password)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	cost, err := bcrypt.Cost([]byte(encoded))
	if err != nil || cost != bcrypt.DefaultCost {
		t.Fatalf("unexpected bcrypt cost: %d, error: %v", cost, err)
	}
	if err := hasher.Verify(encoded, password); err != nil {
		t.Fatalf("verify correct password: %v", err)
	}
	if err := hasher.Verify(encoded, "wrong password"); !errors.Is(err, ErrPasswordMismatch) {
		t.Fatalf("expected password mismatch, got: %v", err)
	}
	second, err := hasher.Hash(password)
	if err != nil {
		t.Fatalf("hash repeated password: %v", err)
	}
	if second == encoded {
		t.Fatal("password hashing did not generate a fresh salt")
	}
}

func TestValidatePasswordBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		password string
		valid    bool
	}{
		{"empty", "", false},
		{"short_ascii", strings.Repeat("a", 11), false},
		{"minimum_ascii", strings.Repeat("a", 12), true},
		{"maximum_ascii", strings.Repeat("a", 72), true},
		{"overlong_ascii", strings.Repeat("a", 73), false},
		{"short_chinese", strings.Repeat("密", 11), false},
		{"minimum_chinese", strings.Repeat("密", 12), true},
		{"maximum_chinese", strings.Repeat("密", 24), true},
		{"overlong_chinese", strings.Repeat("密", 25), false},
		{"short_emoji", strings.Repeat("😀", 11), false},
		{"minimum_emoji", strings.Repeat("😀", 12), true},
		{"maximum_emoji", strings.Repeat("😀", 18), true},
		{"overlong_emoji", strings.Repeat("😀", 19), false},
		{"mixed_boundary", strings.Repeat("密", 23) + "abc", true},
		{"mixed_overlong", strings.Repeat("密", 23) + "abcd", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidatePassword(tc.password)
			if (err == nil) != tc.valid {
				t.Fatalf("unexpected validity: %v", err)
			}
			if !tc.valid {
				if hash, err := NewBcrypt().Hash(tc.password); err == nil || hash != "" {
					t.Fatal("invalid password was hashed")
				}
			}
		})
	}
}

func TestBcryptPreservesPasswordsAtByteLimit(t *testing.T) {
	for _, password := range []string{strings.Repeat("a", 72), strings.Repeat("密", 24), strings.Repeat("😀", 18)} {
		hasher := NewBcrypt()
		hash, err := hasher.Hash(password)
		if err != nil {
			t.Fatalf("hash boundary password: %v", err)
		}
		if err := hasher.Verify(hash, password); err != nil {
			t.Fatalf("verify boundary password: %v", err)
		}
		if err := hasher.Verify(hash, password+"x"); !errors.Is(err, ErrPasswordMismatch) {
			t.Fatalf("overlong password with a matching prefix was not rejected: %v", err)
		}
	}
}

func TestBcryptDoesNotTrimPasswords(t *testing.T) {
	password := "  correct password  "
	hash, err := NewBcrypt().Hash(password)
	if err != nil {
		t.Fatalf("hash password with spaces: %v", err)
	}
	if err := NewBcrypt().Verify(hash, password); err != nil {
		t.Fatalf("verify password with spaces: %v", err)
	}
	if err := NewBcrypt().Verify(hash, strings.TrimSpace(password)); !errors.Is(err, ErrPasswordMismatch) {
		t.Fatalf("password whitespace was ignored: %v", err)
	}
}

func TestBcryptRejectsUnsupportedAndMalformedHashes(t *testing.T) {
	for _, hash := range []string{
		"",
		"not-a-password-hash",
		"$2a$10$broken",
		"$argon2id$v=19$m=65536,t=3,p=2$c2FsdHNhbHQ$aGFzaGhhc2hoYXNoaGFzaA",
	} {
		err := NewBcrypt().Verify(hash, "correct password")
		if err == nil || errors.Is(err, ErrPasswordMismatch) {
			t.Fatalf("expected unsupported or malformed stored hash error, got: %v", err)
		}
	}
}

func TestDummyPasswordHashUsesBcryptDefaultCost(t *testing.T) {
	cost, err := bcrypt.Cost([]byte(DummyPasswordHash()))
	if err != nil || cost != bcrypt.DefaultCost {
		t.Fatalf("dummy bcrypt cost does not match new passwords: %d, error: %v", cost, err)
	}
	if err := NewBcrypt().Verify(DummyPasswordHash(), "dummy password attempt"); !errors.Is(err, ErrPasswordMismatch) {
		t.Fatalf("dummy hash did not execute password comparison: %v", err)
	}
}
