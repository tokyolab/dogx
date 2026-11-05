package jobexecutor

import (
	"crypto/subtle"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Authenticator checks the fixed credentials of one executor group. It is safe
// for concurrent use after construction and never includes credentials in an
// error. Authentication applies to execution methods, not server health checks.
type Authenticator struct {
	groupName string
	token     string
}

// ValidateCredentials checks startup configuration without normalizing it.
// A group name is an exact, case-sensitive identifier of at most 128 Unicode
// characters. A token is a lowercase RFC 4122 UUIDv4 with its hyphens removed.
func ValidateCredentials(groupName, token string) error {
	if !validExecutionName(groupName) {
		return errors.New("executor group name is invalid")
	}
	if len(token) != 32 {
		return errors.New("executor group token is invalid")
	}
	for _, ch := range token {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return errors.New("executor group token is invalid")
		}
	}
	parsed, err := uuid.Parse(token)
	if err != nil || parsed.Version() != 4 || parsed.Variant() != uuid.RFC4122 {
		return errors.New("executor group token is invalid")
	}
	return nil
}

// NewAuthenticator validates and retains immutable group credentials.
func NewAuthenticator(groupName, token string) (*Authenticator, error) {
	if err := ValidateCredentials(groupName, token); err != nil {
		return nil, err
	}
	return &Authenticator{groupName: groupName, token: token}, nil
}

// Authenticate returns Unauthenticated for absent credentials or an incorrect
// token, and PermissionDenied for a different target group. The comparison does
// not trim or case-fold either credential. Tokens are compared in constant time
// for equal-length inputs; a token's fixed public length is not a secret.
func (a *Authenticator) Authenticate(groupName, token string) error {
	if groupName == "" || token == "" {
		return status.Error(codes.Unauthenticated, "executor credentials are required")
	}
	if groupName != a.groupName {
		return status.Error(codes.PermissionDenied, "executor group is not permitted")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(a.token)) != 1 {
		return status.Error(codes.Unauthenticated, "executor credentials are invalid")
	}
	return nil
}

func validExecutionName(name string) bool {
	return name != "" && utf8.ValidString(name) && !strings.ContainsRune(name, 0) &&
		name == strings.TrimSpace(name) && utf8.RuneCountInString(name) <= 128
}

func validExecutorInstanceID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.Version() == 4 && parsed.Variant() == uuid.RFC4122 &&
		value == parsed.String()
}
