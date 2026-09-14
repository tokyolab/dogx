package authn

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestBcryptHashAndVerify(t *testing.T) {
	hasher := NewBcrypt()
	password := "Valid-pass123"
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
		{"too_short", "Abc123!", false},
		{"minimum", "Abcd123!", true},
		{"maximum", "Aa1" + strings.Repeat("!", 29), true},
		{"too_long", "Aa1" + strings.Repeat("!", 30), false},
		{"upper_lower_digit", "Abcd1234", true},
		{"upper_lower_special", "Abcdefg!", true},
		{"upper_digit_special", "ABCD123!", true},
		{"lower_digit_special", "abcd123!", true},
		{"one_category", "abcdefgh", false},
		{"two_letter_categories", "Abcdefgh", false},
		{"lower_digit_only", "abcd1234", false},
		{"upper_digit_only", "ABCD1234", false},
		{"lower_special_only", "abcdefg!", false},
		{"upper_special_only", "ABCDEFG!", false},
		{"digit_special_only", "1234567!", false},
		{"space", "Abc123! ", false},
		{"leading_space", " Abcd123!", false},
		{"tab", "Abc123!\t", false},
		{"newline", "Abc123!\n", false},
		{"chinese", "Abc123!密", false},
		{"emoji", "Abc123!😀", false},
		{"full_width", "Abc123!Ａ", false},
		{"unsupported_punctuation", "Abc123!?", false},
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

func TestValidatePasswordAllowedCharacters(t *testing.T) {
	for ch := rune(0); ch < 128; ch++ {
		allowed := (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') || strings.ContainsRune("!@#$%^&*()_+-=", ch)
		if err := ValidatePassword("Abcd123!" + string(ch)); (err == nil) != allowed {
			t.Errorf("unexpected validity for character %q: %v", ch, err)
		}
	}
}

func TestBcryptVerifiesExistingPasswordsAtByteLimit(t *testing.T) {
	for _, password := range []string{strings.Repeat("a", 72), strings.Repeat("密", 24), strings.Repeat("😀", 18)} {
		hasher := NewBcrypt()
		// Existing hashes are not subject to the policy for setting new passwords.
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
		if err != nil {
			t.Fatalf("hash boundary password: %v", err)
		}
		if err := hasher.Verify(string(hash), password); err != nil {
			t.Fatalf("verify boundary password: %v", err)
		}
		if err := hasher.Verify(string(hash), password+"x"); !errors.Is(err, ErrPasswordMismatch) {
			t.Fatalf("overlong password with a matching prefix was not rejected: %v", err)
		}
	}
}

func TestBcryptDoesNotTrimExistingPasswords(t *testing.T) {
	password := "  correct password  "
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hash password with spaces: %v", err)
	}
	if err := NewBcrypt().Verify(string(hash), password); err != nil {
		t.Fatalf("verify password with spaces: %v", err)
	}
	if err := NewBcrypt().Verify(string(hash), strings.TrimSpace(password)); !errors.Is(err, ErrPasswordMismatch) {
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
