package rpchost

import (
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/zeromicro/go-zero/core/discov"
)

// ValidateListener requires an explicit, fixed TCP port for a registered RPC
// listener. It does not resolve the host or open a socket. Errors omit the
// supplied value because malformed configuration can contain credentials.
func ValidateListener(address string) error {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return errors.New("RPC ListenOn must be a host and port")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number <= 0 || number > 65535 {
		return errors.New("RPC ListenOn must use a fixed port between 1 and 65535")
	}
	return nil
}

// ValidateDiscovery requires native etcd Hosts and Key settings and rejects
// blank values. Endpoint syntax and connectivity remain owned by the native
// etcd client; this function makes no network requests and reports no values.
func ValidateDiscovery(config discov.EtcdConf) error {
	if err := config.Validate(); err != nil {
		return errors.New("RPC discovery requires etcd Hosts and Key")
	}
	if strings.TrimSpace(config.Key) == "" {
		return errors.New("RPC discovery requires a nonempty etcd Key")
	}
	for _, endpoint := range config.Hosts {
		if strings.TrimSpace(endpoint) == "" {
			return errors.New("RPC discovery Hosts must not contain empty endpoints")
		}
	}
	return nil
}

// ListenersOverlap reports a known literal address overlap, including wildcard
// bindings and equivalent IP spellings. It does not resolve DNS aliases; false
// does not prove that both listeners can bind on the deployment host. An IPv6
// wildcard is treated as potentially accepting IPv4 on a dual-stack host.
func ListenersOverlap(left, right string) bool {
	leftHost, leftPort, leftErr := net.SplitHostPort(left)
	rightHost, rightPort, rightErr := net.SplitHostPort(right)
	if leftErr != nil || rightErr != nil || canonicalPort(leftPort) != canonicalPort(rightPort) {
		return false
	}
	leftHost, rightHost = canonicalHost(leftHost), canonicalHost(rightHost)
	if leftHost == rightHost || leftHost == "" || rightHost == "" {
		return true
	}
	leftIP, leftErr := netip.ParseAddr(leftHost)
	rightIP, rightErr := netip.ParseAddr(rightHost)
	if leftErr != nil || rightErr != nil {
		return false
	}
	return leftIP.IsUnspecified() && (leftIP.Is6() || rightIP.Is4()) ||
		rightIP.IsUnspecified() && (rightIP.Is6() || leftIP.Is4())
}

// DiscoveryOverlap reports equal keys with at least one known shared etcd
// endpoint. Different endpoint sets may still belong to the same cluster; this
// static check neither discovers cluster membership nor resolves DNS aliases.
// Deployments must verify those remaining cases before reusing a key for
// different services.
func DiscoveryOverlap(left, right discov.EtcdConf) bool {
	if left.Key != right.Key {
		return false
	}
	for _, a := range left.Hosts {
		for _, b := range right.Hosts {
			if canonicalEndpoint(a) == canonicalEndpoint(b) {
				return true
			}
		}
	}
	return false
}

func canonicalEndpoint(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") &&
		(parsed.Path == "" || parsed.Path == "/") && parsed.RawQuery == "" && parsed.Fragment == "" {
		endpoint = parsed.Host
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return endpoint
	}
	return net.JoinHostPort(canonicalHost(host), canonicalPort(port))
}

func canonicalHost(host string) string {
	if address, err := netip.ParseAddr(host); err == nil {
		return address.Unmap().String()
	}
	return strings.ToLower(host)
}

func canonicalPort(port string) string {
	if number, err := strconv.Atoi(port); err == nil {
		return strconv.Itoa(number)
	}
	return port
}
