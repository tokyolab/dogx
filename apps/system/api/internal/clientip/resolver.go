package clientip

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

type Resolver struct{ trusted []netip.Prefix }

func New(cidrs []string) (*Resolver, error) {
	r := &Resolver{}
	for _, cidr := range cidrs {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, err
		}
		r.trusted = append(r.trusted, prefix)
	}
	return r, nil
}

func (r *Resolver) trusts(ip netip.Addr) bool {
	// Empty configuration deliberately trusts all proxies. Deployments using
	// this default must isolate the API and sanitize headers at their ingress.
	if r == nil || len(r.trusted) == 0 {
		return true
	}
	for _, prefix := range r.trusted {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}

func (r *Resolver) Resolve(req *http.Request) string {
	host := req.RemoteAddr
	if parsed, _, err := net.SplitHostPort(host); err == nil {
		host = parsed
	}
	peer, err := netip.ParseAddr(strings.Trim(host, "[]"))
	if err != nil {
		return ""
	}
	peer = peer.Unmap()
	if !r.trusts(peer) {
		return peer.String()
	}
	// Multiple header lines are one forwarding chain; reading only the first
	// could omit the peer address appended by a trusted proxy.
	forwarded := strings.Join(req.Header.Values("X-Forwarded-For"), ",")
	if forwarded == "" {
		if len(req.Header.Values("X-Real-IP")) > 1 {
			return peer.String()
		}
		forwarded = req.Header.Get("X-Real-IP")
	}
	if forwarded == "" {
		return peer.String()
	}
	var chain []netip.Addr
	for _, raw := range strings.Split(forwarded, ",") {
		ip, err := netip.ParseAddr(strings.TrimSpace(raw))
		if err != nil {
			return peer.String()
		}
		chain = append(chain, ip.Unmap())
	}
	// With an allowlist, stop at the first untrusted hop from the right.
	// In trust-all mode, use the leftmost address supplied by the ingress.
	for i := len(chain) - 1; i >= 0; i-- {
		if !r.trusts(chain[i]) || i == 0 {
			return chain[i].String()
		}
	}
	return peer.String()
}
