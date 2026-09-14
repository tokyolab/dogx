package authn

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

const (
	MinPasswordCharacters = 8
	MaxPasswordCharacters = 32
	// Keep bcrypt's byte limit for verification of existing passwords, which
	// may predate the current new-password policy.
	MaxPasswordBytes = 72
)

var ErrPasswordMismatch = errors.New("password mismatch")

// This synthetic hash is only a workload for unknown usernames, never an
// account credential. Its cost must match the cost used for new passwords.
const dummyPasswordHash = "$2a$10$1sbpKmhQDpXLHnDnEQ1nLe3oOnYyP2bUJyqHcX2T0Fq1qfyoXOrPm"

type PasswordVerifier interface {
	Verify(encodedHash, password string) error
}

type PasswordHasher interface {
	PasswordVerifier
	Hash(password string) (string, error)
}

type Bcrypt struct{}

func ValidatePassword(password string) error {
	// Only ASCII is accepted below, so each permitted character is one byte.
	if len(password) < MinPasswordCharacters || len(password) > MaxPasswordCharacters {
		return fmt.Errorf("password must contain %d to %d characters",
			MinPasswordCharacters, MaxPasswordCharacters)
	}
	var categories [4]bool
	for _, ch := range password {
		switch {
		case ch >= 'A' && ch <= 'Z':
			categories[0] = true
		case ch >= 'a' && ch <= 'z':
			categories[1] = true
		case ch >= '0' && ch <= '9':
			categories[2] = true
		case strings.ContainsRune("!@#$%^&*()_+-=", ch):
			categories[3] = true
		default:
			return errors.New("password may only contain English letters, digits and !@#$%^&*()_+-=")
		}
	}
	count := 0
	for _, present := range categories {
		if present {
			count++
		}
	}
	if count < 3 {
		return errors.New("password must contain at least three of uppercase letters, lowercase letters, digits and special characters")
	}
	return nil
}

func NewBcrypt() *Bcrypt {
	return &Bcrypt{}
}

func (b *Bcrypt) Hash(password string) (string, error) {
	if err := ValidatePassword(password); err != nil {
		return "", err
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash bcrypt password: %w", err)
	}
	return string(hash), nil
}

func DummyPasswordHash() string {
	return dummyPasswordHash
}

func (b *Bcrypt) Verify(encodedHash, password string) error {
	// GenerateFromPassword enforces the byte limit, but comparison must also
	// reject overlong inputs explicitly instead of accepting a matching prefix.
	if len(password) > MaxPasswordBytes {
		return ErrPasswordMismatch
	}
	err := bcrypt.CompareHashAndPassword([]byte(encodedHash), []byte(password))
	if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
		return ErrPasswordMismatch
	}
	if err != nil {
		return fmt.Errorf("verify bcrypt password: %w", err)
	}
	return nil
}
