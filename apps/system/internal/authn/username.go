package authn

import (
	"errors"
	"fmt"
	"regexp"
)

const MaxUsernameCharacters = 64

var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9]+(-[A-Za-z0-9]+)*$`)

func ValidateUsername(username string) error {
	// The format only permits ASCII, so the character and byte limits agree.
	if len(username) == 0 || len(username) > MaxUsernameCharacters {
		return fmt.Errorf("username must contain 1 to %d characters", MaxUsernameCharacters)
	}
	if !usernamePattern.MatchString(username) {
		return errors.New("username may only contain English letters, digits and single hyphens, and must not begin or end with a hyphen")
	}
	return nil
}
