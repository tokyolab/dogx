package authn

import "context"

type roleProviderStub struct {
	roleIDs      []int64
	isSuperAdmin bool
	err          error
	superErr     error
}

func (s *roleProviderStub) ListEnabledRoleIDs(context.Context, int64) ([]int64, error) {
	return append([]int64(nil), s.roleIDs...), s.err
}

func (s *roleProviderStub) IsSuperAdmin(context.Context, int64) (bool, error) {
	return s.isSuperAdmin, s.superErr
}

func testRoleProvider() RoleProvider {
	return &roleProviderStub{roleIDs: []int64{2, 7}}
}
