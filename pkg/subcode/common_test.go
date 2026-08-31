package subcode_test

import (
	"testing"

	"github.com/tokyolab/dogx/pkg/i18n"
	"github.com/tokyolab/dogx/pkg/subcode"
)

func TestCommonSubcodesHaveAllTranslations(t *testing.T) {
	keys := []string{
		subcode.InvalidRequest,
		subcode.AuthenticationRequired,
		subcode.PermissionDenied,
		subcode.ResourceNotFound,
		subcode.RequestCanceled,
		subcode.RequestConflict,
		subcode.TooManyRequests,
		subcode.NotImplemented,
		subcode.ServiceUnavailable,
		subcode.RequestTimeout,
		subcode.InternalError,
		subcode.BusinessError,
	}
	for _, key := range keys {
		for _, locale := range []string{i18n.ZhCN, i18n.EnUS} {
			if message, ok := i18n.LookupLocale(locale, key); !ok || message == "" {
				t.Errorf("missing %s translation for %s", locale, key)
			}
		}
	}
}
