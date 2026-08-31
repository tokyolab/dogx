package logic

import (
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/tokyolab/dogx/apps/system/internal/model"
)

const (
	maxRoleCodeCharacters   = 64
	maxRoleNameRunes        = 64
	maxRoleDescriptionRunes = 500
)

var roleCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

type normalizedRoleInput struct {
	code        string
	name        string
	description string
}

func normalizeRoleInput(code, name, description string) (normalizedRoleInput, error) {
	result := normalizedRoleInput{
		code:        strings.ToLower(strings.TrimSpace(code)),
		name:        strings.TrimSpace(name),
		description: strings.TrimSpace(description),
	}
	if result.code == "" || utf8.RuneCountInString(result.code) > maxRoleCodeCharacters ||
		!roleCodePattern.MatchString(result.code) {
		return normalizedRoleInput{}, errors.New("invalid role code")
	}
	if result.name == "" || utf8.RuneCountInString(result.name) > maxRoleNameRunes {
		return normalizedRoleInput{}, errors.New("invalid role name")
	}
	if utf8.RuneCountInString(result.description) > maxRoleDescriptionRunes {
		return normalizedRoleInput{}, errors.New("invalid role description")
	}
	return result, nil
}

func validRecordStatus(value int32) bool {
	status := model.RecordStatus(value)
	return status == model.RecordStatusDisabled || status == model.RecordStatusEnabled
}
