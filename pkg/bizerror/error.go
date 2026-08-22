package bizerror

import "errors"

const (
	MinCode     uint32 = 100000
	DefaultCode uint32 = 100001
	MaxCode     uint32 = 1<<31 - 1
)

type Error struct {
	code    uint32
	message string
}

func New(message string) *Error {
	return NewCode(DefaultCode, message)
}

func NewCode(code uint32, message string) *Error {
	return &Error{
		code:    code,
		message: message,
	}
}

func (e *Error) Error() string {
	return e.message
}

func (e *Error) Code() uint32 {
	return e.code
}

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
