package authn

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"golang.org/x/crypto/bcrypt"
)

const (
	MinPasswordCharacters = 12
	// bcrypt limits passwords by bytes, not Unicode characters.
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
	if utf8.RuneCountInString(password) < MinPasswordCharacters || len(password) > MaxPasswordBytes {
		return fmt.Errorf("password must contain at least %d characters and at most %d UTF-8 bytes",
			MinPasswordCharacters, MaxPasswordBytes)
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
