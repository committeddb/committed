package cluster

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// CanonicalPeerURL validates a raft peer URL and returns its one canonical
// spelling, scheme://host:port: lower-case scheme and host, an explicit port
// (80 for http, 443 for https when none is given), and nothing else — no
// path, query, user info or fragment. A trailing "/" is accepted and dropped.
//
// Every place a peer URL enters the system (COMMITTED_PEERS, the membership
// API, recorded addresses) goes through it, so two spellings of one address
// compare equal, a cluster id derived from the peer set is the same on every
// node, and the transport can append its paths without producing "//raft".
func CanonicalPeerURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("peer URL is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("peer URL %q does not parse: %w", raw, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("peer URL %q must use http or https", raw)
	}
	if u.User != nil {
		return "", fmt.Errorf("peer URL %q must not carry user info", raw)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", fmt.Errorf("peer URL %q must not have a query or fragment", raw)
	}
	if u.Path != "" && u.Path != "/" {
		return "", fmt.Errorf("peer URL %q must not have a path (the transport appends its own)", raw)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", fmt.Errorf("peer URL %q has no host", raw)
	}
	port := u.Port()
	if port == "" {
		if strings.HasSuffix(u.Host, ":") {
			return "", fmt.Errorf("peer URL %q has an empty port", raw)
		}
		port = "80"
		if scheme == "https" {
			port = "443"
		}
	}
	return scheme + "://" + net.JoinHostPort(host, port), nil
}
