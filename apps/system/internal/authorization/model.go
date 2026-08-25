package authorization

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/casbin/casbin/v3/model"
)

const policyModel = `[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.sub == p.sub && r.obj == p.obj && r.act == p.act
`

var ErrInvalidRoleID = errors.New("invalid role id")

func NewModel() (model.Model, error) {
	policy, err := model.NewModelFromString(policyModel)
	if err != nil {
		return nil, fmt.Errorf("parse authorization model: %w", err)
	}
	return policy, nil
}

func RoleSubject(roleID int64) (string, error) {
	if roleID <= 0 {
		return "", ErrInvalidRoleID
	}
	return "r:" + strconv.FormatInt(roleID, 10), nil
}

func PolicyRule(roleID int64, path, method string) ([]string, error) {
	subject, err := RoleSubject(roleID)
	if err != nil {
		return nil, err
	}
	path = strings.TrimSpace(path)
	method = strings.ToUpper(strings.TrimSpace(method))
	if path == "" || !strings.HasPrefix(path, "/") {
		return nil, errors.New("authorization path must start with slash")
	}
	if method == "" {
		return nil, errors.New("authorization method is empty")
	}
	return []string{subject, path, method}, nil
}
