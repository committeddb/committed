package cmd

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMemberBaseURL verifies target resolution: an explicit --target wins
// (trailing slash trimmed), otherwise it derives from COMMITTED_API_ADDR the
// same way the healthcheck probe does (wildcard host → 127.0.0.1, https when
// the local API serves TLS).
func TestMemberBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		target  string // --target
		addr    string // COMMITTED_API_ADDR; "" means unset
		tlsCert bool   // COMMITTED_HTTP_TLS_CERT_FILE set
		want    string
	}{
		{name: "explicit target wins", target: "http://n1:8080", want: "http://n1:8080"},
		{name: "explicit target trailing slash trimmed", target: "http://n1:8080/", want: "http://n1:8080"},
		{name: "unset defaults to loopback 8080", want: "http://127.0.0.1:8080"},
		{name: "wildcard host rewritten", addr: ":9000", want: "http://127.0.0.1:9000"},
		{name: "explicit host preserved", addr: "10.0.0.5:8080", want: "http://10.0.0.5:8080"},
		{name: "tls cert flips scheme to https", addr: ":8443", tlsCert: true, want: "https://127.0.0.1:8443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memberTarget = tt.target
			t.Cleanup(func() { memberTarget = "" })
			if tt.addr != "" {
				t.Setenv("COMMITTED_API_ADDR", tt.addr)
			}
			if tt.tlsCert {
				t.Setenv("COMMITTED_HTTP_TLS_CERT_FILE", "/some/cert.pem")
			}
			got, err := memberBaseURL()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestMemberAdd_RequiresIDAndURL verifies the add command rejects a missing
// id or url before making any request.
func TestMemberAdd_RequiresIDAndURL(t *testing.T) {
	memberID, memberURL = 0, ""
	t.Cleanup(func() { memberID, memberURL = 0, "" })

	require.Error(t, memberAddCmd.RunE(memberAddCmd, nil), "missing id must error")

	memberID, memberURL = 4, ""
	require.Error(t, memberAddCmd.RunE(memberAddCmd, nil), "missing url must error")
}

// TestMemberRemove_RequiresID verifies the remove command rejects a missing id.
func TestMemberRemove_RequiresID(t *testing.T) {
	memberID = 0
	t.Cleanup(func() { memberID = 0 })
	require.Error(t, memberRemoveCmd.RunE(memberRemoveCmd, nil))
}

// TestMemberDo drives memberDo against a local server, asserting the method,
// path, JSON body, and bearer header are sent, and that a 2xx maps to nil
// while a non-2xx maps to an error carrying the server's status.
func TestMemberDo(t *testing.T) {
	var gotMethod, gotPath, gotAuth string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotAuth = r.Method, r.URL.Path, r.Header.Get("Authorization")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	memberTarget = srv.URL
	memberToken = "s3cret"
	t.Cleanup(func() { memberTarget, memberToken = "", "" })

	body, _ := json.Marshal(map[string]any{"id": 4, "url": "http://n4:9022"})
	require.NoError(t, memberDo(http.MethodPost, "/v1/membership", body))

	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/v1/membership", gotPath)
	require.Equal(t, "Bearer s3cret", gotAuth)
	require.JSONEq(t, `{"id":4,"url":"http://n4:9022"}`, string(gotBody))
}

// TestMemberDo_ServerError verifies a non-2xx response becomes an error that
// includes the status code and the server's response body.
func TestMemberDo_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"membership_unconfirmed","message":"nope"}`))
	}))
	defer srv.Close()

	memberTarget = srv.URL
	t.Cleanup(func() { memberTarget = "" })

	err := memberDo(http.MethodDelete, "/v1/membership/4", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "503")
	require.Contains(t, err.Error(), "membership_unconfirmed")
}
