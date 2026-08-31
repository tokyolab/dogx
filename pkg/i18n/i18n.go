package i18n

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/text/language"
)

const (
	ZhCN = "zh-CN"
	EnUS = "en-US"
)

var (
	//go:embed locales/*.json
	localeFiles embed.FS

	catalogs = mustLoadCatalogs()
	matcher  = language.NewMatcher([]language.Tag{
		language.MustParse(ZhCN),
		language.MustParse(EnUS),
	})
)

type localeContextKey struct{}

func Middleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := WithLocale(r.Context(), Resolve(r.Header.Get("Accept-Language")))
		next(w, r.WithContext(ctx))
	}
}

func WithLocale(ctx context.Context, locale string) context.Context {
	return context.WithValue(ctx, localeContextKey{}, normalize(locale))
}

func Locale(ctx context.Context) string {
	if ctx != nil {
		if locale, ok := ctx.Value(localeContextKey{}).(string); ok {
			return normalize(locale)
		}
	}
	return ZhCN
}

func Resolve(acceptLanguage string) string {
	if strings.TrimSpace(acceptLanguage) == "" {
		return ZhCN
	}
	_, index := language.MatchStrings(matcher, acceptLanguage)
	if index == 1 {
		return EnUS
	}
	return ZhCN
}

func Lookup(ctx context.Context, key string) (string, bool) {
	return LookupLocale(Locale(ctx), key)
}

func LookupLocale(locale, key string) (string, bool) {
	locale = normalize(locale)
	if message, ok := catalogs[locale][key]; ok {
		return message, true
	}
	message, ok := catalogs[ZhCN][key]
	return message, ok
}

func normalize(locale string) string {
	if strings.EqualFold(locale, EnUS) {
		return EnUS
	}
	return ZhCN
}

func mustLoadCatalogs() map[string]map[string]string {
	result := make(map[string]map[string]string, 2)
	for _, locale := range []string{ZhCN, EnUS} {
		data, err := localeFiles.ReadFile("locales/" + locale + ".json")
		if err != nil {
			panic(fmt.Sprintf("read %s locale: %v", locale, err))
		}
		var nested map[string]any
		if err := json.Unmarshal(data, &nested); err != nil {
			panic(fmt.Sprintf("decode %s locale: %v", locale, err))
		}
		flat := make(map[string]string)
		flatten(flat, "", nested)
		result[locale] = flat
	}
	return result
}

func flatten(target map[string]string, prefix string, source map[string]any) {
	for key, value := range source {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		switch typed := value.(type) {
		case string:
			target[path] = typed
		case map[string]any:
			flatten(target, path, typed)
		default:
			panic(fmt.Sprintf("locale key %s must contain a string or object", path))
		}
	}
}
