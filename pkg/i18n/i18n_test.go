package i18n

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolveAcceptLanguage(t *testing.T) {
	tests := []struct {
		header string
		want   string
	}{
		{header: "", want: ZhCN},
		{header: "zh-CN", want: ZhCN},
		{header: "en-US", want: EnUS},
		{header: "en-US,en;q=0.9,zh-CN;q=0.8", want: EnUS},
		{header: "fr-FR,zh-CN;q=0.9", want: ZhCN},
	}
	for _, test := range tests {
		if got := Resolve(test.header); got != test.want {
			t.Errorf("Resolve(%q) = %q, want %q", test.header, got, test.want)
		}
	}
}

func TestLookupFallsBackToChinese(t *testing.T) {
	if got, ok := LookupLocale("unsupported", "common.success"); !ok || got == "" {
		t.Fatalf("fallback translation = %q, %t", got, ok)
	}
	if _, ok := LookupLocale(EnUS, "missing.translation"); ok {
		t.Fatal("missing translation unexpectedly resolved")
	}
}

func TestMiddlewareStoresResolvedLocale(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Language", "en-US")
	recorder := httptest.NewRecorder()
	var got string
	Middleware(func(_ http.ResponseWriter, r *http.Request) {
		got = Locale(r.Context())
	})(recorder, request)
	if got != EnUS {
		t.Fatalf("middleware locale = %q, want %q", got, EnUS)
	}
	if Locale(context.Background()) != ZhCN {
		t.Fatal("default context locale is not Chinese")
	}
}
