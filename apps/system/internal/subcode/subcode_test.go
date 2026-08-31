package subcode

import (
	"testing"

	"github.com/tokyolab/dogx/pkg/i18n"
)

func TestSystemSubcodesHaveAllTranslations(t *testing.T) {
	keys := []string{
		AuthInvalidCredentials,
		AuthUserDisabled,
		AuthNewPasswordUnchanged,
		AuthCurrentPasswordWrong,
		RoleNotFound,
		RoleCodeExists,
		RoleCodeReserved,
		RoleInUse,
		RoleSystemCannotDelete,
		RoleSystemCannotDisable,
		RoleSystemCodeImmutable,
		RoleUnavailable,
		RoleAPIUnavailable,
		RoleSuperAdminAPIProtected,
	}
	for _, key := range keys {
		for _, locale := range []string{i18n.ZhCN, i18n.EnUS} {
			if message, ok := i18n.LookupLocale(locale, key); !ok || message == "" {
				t.Errorf("missing %s translation for %s", locale, key)
			}
		}
	}
}
