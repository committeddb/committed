package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalPeerURL(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"http://n1:9022", "http://n1:9022"},
		{"http://n1:9022/", "http://n1:9022"}, // the trailing slash that stopped every commit
		{"  HTTP://N1.Example.COM:9022  ", "http://n1.example.com:9022"},
		{"http://n1", "http://n1:80"},
		{"https://n1", "https://n1:443"},
		{"http://10.0.0.5:9022", "http://10.0.0.5:9022"},
		{"http://[::1]:9022", "http://[::1]:9022"},
		{"https://[FE80::1]", "https://[fe80::1]:443"},
	} {
		got, err := CanonicalPeerURL(tc.in)
		require.NoError(t, err, tc.in)
		require.Equal(t, tc.want, got, tc.in)
	}
}

func TestCanonicalPeerURLRefuses(t *testing.T) {
	for _, in := range []string{
		"",
		"n1:9022",              // no scheme
		"ftp://n1:9022",        // wrong scheme
		"http://:9022",         // no host
		"http://n1:",           // empty port
		"http://n1:9022/raft",  // a path
		"http://n1:9022?x=1",   // a query
		"http://n1:9022#frag",  // a fragment
		"http://u:p@n1:9022",   // user info
		"http://n1:9022/x/y/z", // a deep path
		"http://n1:notaport",   // a bad port
	} {
		_, err := CanonicalPeerURL(in)
		require.Error(t, err, "%q must be refused", in)
	}
}
