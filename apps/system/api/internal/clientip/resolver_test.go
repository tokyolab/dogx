package clientip

import (
	"net/http/httptest"
	"testing"
)

func TestResolver(t *testing.T) {
	trusted, err := New([]string{"10.0.0.0/8", "::1/128"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, peer, forwarded, real, want string }{
		{"direct spoof", "192.0.2.1:80", "198.51.100.1", "", "192.0.2.1"},
		{"trusted chain", "10.0.0.1:80", "198.51.100.1, 10.0.0.2", "", "198.51.100.1"},
		{"untrusted intermediary", "10.0.0.1:80", "1.1.1.1, 198.51.100.2", "", "198.51.100.2"},
		{"invalid chain", "10.0.0.1:80", "not-ip, 198.51.100.2", "", "10.0.0.1"},
		{"ipv6", "[::1]:80", "2001:db8::1", "", "2001:db8::1"},
		{"real ip", "10.0.0.1:80", "", "198.51.100.1", "198.51.100.1"},
		{"no header", "10.0.0.1:80", "", "", "10.0.0.1"},
		{"invalid peer", "bad", "198.51.100.1", "", ""},
		{"mapped ipv4", "[::ffff:192.0.2.1]:80", "1.1.1.1", "", "192.0.2.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", nil)
			req.RemoteAddr = tc.peer
			req.Header.Set("X-Forwarded-For", tc.forwarded)
			req.Header.Set("X-Real-IP", tc.real)
			if got := trusted.Resolve(req); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
	if resolver, err := New([]string{"10.0.0.0/8", "invalid"}); err == nil || resolver != nil {
		t.Fatal("invalid proxy must reject the entire configuration")
	}
	req := httptest.NewRequest("POST", "/", nil)
	req.RemoteAddr = "10.0.0.1:80"
	req.Header.Set("X-Forwarded-For", "1.1.1.1")
	var none *Resolver
	if none.Resolve(req) != "1.1.1.1" {
		t.Fatal("nil resolver must use the default trust-all policy")
	}
	req.Header.Add("X-Forwarded-For", "192.0.2.9")
	if trusted.Resolve(req) != "192.0.2.9" {
		t.Fatal("ignored last forwarding header")
	}
	req.Header.Del("X-Forwarded-For")
	req.Header.Add("X-Real-IP", "1.1.1.1")
	req.Header.Add("X-Real-IP", "192.0.2.9")
	if trusted.Resolve(req) != "10.0.0.1" {
		t.Fatal("trusted ambiguous real IP")
	}
}

func TestDefaultResolverTrustsForwardedHeaders(t *testing.T) {
	for _, cidrs := range [][]string{nil, {}} {
		resolver, err := New(cidrs)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct{ name, peer, forwarded, real, want string }{
			{"direct without headers", "192.0.2.1:80", "", "", "192.0.2.1"},
			{"external proxy", "192.0.2.1:80", "198.51.100.1", "", "198.51.100.1"},
			{"multiple proxies", "192.0.2.1:80", "198.51.100.1, 10.0.0.2", "", "198.51.100.1"},
			{"real ip fallback", "192.0.2.1:80", "", "198.51.100.2", "198.51.100.2"},
			{"forwarded takes priority", "192.0.2.1:80", "198.51.100.1", "198.51.100.2", "198.51.100.1"},
			{"ipv6", "[2001:db8::2]:80", "2001:db8::1", "", "2001:db8::1"},
			{"invalid header", "192.0.2.1:80", "bad", "", "192.0.2.1"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest("POST", "/", nil)
				req.RemoteAddr = tc.peer
				req.Header.Set("X-Forwarded-For", tc.forwarded)
				req.Header.Set("X-Real-IP", tc.real)
				if got := resolver.Resolve(req); got != tc.want {
					t.Fatalf("got %q want %q", got, tc.want)
				}
			})
		}
	}
}
