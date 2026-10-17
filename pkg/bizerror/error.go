package bizerror

import (
	"errors"
	"regexp"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

const (
	MinCode     uint32 = 100000
	DefaultCode uint32 = 100001
	MaxCode     uint32 = 1<<31 - 1
)

// Error represents a business failure that can cross DogX internal RPC boundaries.
// subcode is a stable machine-readable identifier; message is diagnostic text
// that may change and must not control program flow. Extended numeric codes are
// not standard gRPC codes and must not be used directly by public gRPC services.
type Error struct {
	code    uint32
	subcode string
	message string
}

var subcodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$`)

func New(subcode, message string) *Error {
	return NewCode(DefaultCode, subcode, message)
}

// NewCode constructs an error without validating code or subcode. The internal
// RPC interceptor and HTTP response boundary validate these fields and convert
// invalid values into technical errors instead of emitting them as business errors.
func NewCode(code uint32, subcode, message string) *Error {
	return &Error{
		code:    code,
		subcode: subcode,
		message: message,
	}
}

func (e *Error) Error() string {
	return e.message
}

func (e *Error) Code() uint32 {
	return e.code
}

func (e *Error) Subcode() string {
	return e.subcode
}

// From uses errors.As to find a business error in err's chain. It returns
// (nil, false) if no such error is found.
func From(err error) (*Error, bool) {
	var target *Error
	if !errors.As(err, &target) {
		return nil, false
	}

	return target, true
}

func IsCode(code uint32) bool {
	return code >= MinCode && code <= MaxCode
}

func IsSubcode(subcode string) bool {
	return subcodePattern.MatchString(subcode)
}

// SubcodeFromStatus extracts a valid ErrorInfo.Reason only from an extended
// business status. It returns ("", false) for nil, standard gRPC statuses, or a
// missing valid Reason; it never infers an identifier from Message.
func SubcodeFromStatus(st *status.Status) (string, bool) {
	if st == nil || !IsCode(uint32(st.Code())) {
		return "", false
	}
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && IsSubcode(info.Reason) {
			return info.Reason, true
		}
	}
	return "", false
}
