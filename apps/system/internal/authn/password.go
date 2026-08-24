package authn

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

const (
	MinPasswordBytes         = 12
	MaxPasswordBytes         = 128
	argon2Memory      uint32 = 64 * 1024
	argon2Iterations  uint32 = 3
	argon2Parallelism uint8  = 2
	argon2SaltLength         = 16
	argon2KeyLength   uint32 = 32
)

var ErrPasswordMismatch = errors.New("password mismatch")

var dummyPasswordHash = encodeArgon2id(
	[]byte("dogx-invalid-password"),
	[]byte("dogx-dummy-salt"),
)

type PasswordVerifier interface {
	Verify(encodedHash, password string) error
}

type PasswordHasher interface {
	PasswordVerifier
	Hash(password string) (string, error)
}

type Argon2id struct{}

func ValidatePassword(password string) error {
	if len(password) < MinPasswordBytes || len(password) > MaxPasswordBytes {
		return fmt.Errorf(
			"password must contain %d to %d bytes",
			MinPasswordBytes,
			MaxPasswordBytes,
		)
	}
	return nil
}

func NewArgon2id() *Argon2id {
	return &Argon2id{}
}

func (a *Argon2id) Hash(password string) (string, error) {
	if err := ValidatePassword(password); err != nil {
		return "", err
	}

	salt := make([]byte, argon2SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}

	return encodeArgon2id([]byte(password), salt), nil
}

func DummyPasswordHash() string {
	return dummyPasswordHash
}

func encodeArgon2id(password, salt []byte) string {
	hash := argon2.IDKey(
		password,
		salt,
		argon2Iterations,
		argon2Memory,
		argon2Parallelism,
		argon2KeyLength,
	)

	return fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		argon2Memory,
		argon2Iterations,
		argon2Parallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	)
}

func (a *Argon2id) Verify(encodedHash, password string) error {
	parameters, salt, expected, err := parseArgon2idHash(encodedHash)
	if err != nil {
		return err
	}

	actual := argon2.IDKey(
		[]byte(password),
		salt,
		parameters.iterations,
		parameters.memory,
		parameters.parallelism,
		uint32(len(expected)),
	)
	if subtle.ConstantTimeCompare(expected, actual) != 1 {
		return ErrPasswordMismatch
	}

	return nil
}

type argon2Parameters struct {
	memory      uint32
	iterations  uint32
	parallelism uint8
}

func parseArgon2idHash(encodedHash string) (argon2Parameters, []byte, []byte, error) {
	parts := strings.Split(encodedHash, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return argon2Parameters{}, nil, nil, errors.New("invalid argon2id hash format")
	}

	version, err := parseUintParameter(parts[2], "v=", 8)
	if err != nil || int(version) != argon2.Version {
		return argon2Parameters{}, nil, nil, errors.New("unsupported argon2id version")
	}

	parameters, err := parseArgon2Parameters(parts[3])
	if err != nil {
		return argon2Parameters{}, nil, nil, err
	}

	salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	if err != nil || len(salt) < 8 {
		return argon2Parameters{}, nil, nil, errors.New("invalid argon2id salt")
	}

	hash, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if err != nil || len(hash) < 16 {
		return argon2Parameters{}, nil, nil, errors.New("invalid argon2id hash")
	}

	return parameters, salt, hash, nil
}

func parseArgon2Parameters(value string) (argon2Parameters, error) {
	parts := strings.Split(value, ",")
	if len(parts) != 3 {
		return argon2Parameters{}, errors.New("invalid argon2id parameters")
	}

	memory, err := parseUintParameter(parts[0], "m=", 32)
	if err != nil || memory < 8*1024 || memory > 256*1024 {
		return argon2Parameters{}, errors.New("invalid argon2id memory")
	}

	iterations, err := parseUintParameter(parts[1], "t=", 32)
	if err != nil || iterations == 0 || iterations > 20 {
		return argon2Parameters{}, errors.New("invalid argon2id iterations")
	}

	parallelism, err := parseUintParameter(parts[2], "p=", 8)
	if err != nil || parallelism == 0 || parallelism > 16 {
		return argon2Parameters{}, errors.New("invalid argon2id parallelism")
	}

	return argon2Parameters{
		memory:      uint32(memory),
		iterations:  uint32(iterations),
		parallelism: uint8(parallelism),
	}, nil
}

func parseUintParameter(value, prefix string, bitSize int) (uint64, error) {
	if !strings.HasPrefix(value, prefix) {
		return 0, errors.New("missing argon2id parameter")
	}
	return strconv.ParseUint(strings.TrimPrefix(value, prefix), 10, bitSize)
}
