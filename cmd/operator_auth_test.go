package cmd

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Exercise both outbound clients: standalone operators need only their
// membership token, while legacy installations retain the API-token default.
func TestOperatorCredentials(t *testing.T) {
	archive := nodeArchive(t)
	for _, tc := range []struct{ name, flag, member, api, want string }{
		{name: "development"},
		{name: "legacy", api: " legacy\n", want: "legacy"},
		{name: "standalone membership", member: " member\n", want: "member"},
		{name: "membership wins", member: "member", api: "api", want: "member"},
		{name: "blank membership", member: " \n", api: "legacy", want: "legacy"},
		{name: "explicit override", flag: "override", member: "member", api: "api", want: "override"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COMMITTED_MEMBERSHIP_TOKEN", tc.member)
			t.Setenv("COMMITTED_API_TOKEN", tc.api)
			// Never borrow the infrastructure credential, even if it is the only one.
			t.Setenv("COMMITTED_PEER_TOKEN", "peer-secret")
			headers := make(chan string, 2)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				headers <- r.Header.Get("Authorization")
				switch r.URL.Path {
				case "/v1/membership":
					w.WriteHeader(http.StatusNoContent)
				case nodeBackupPath:
					_, _ = w.Write(archive)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			oldTarget, oldToken, oldInsecure := memberTarget, memberToken, memberInsecure
			t.Cleanup(func() { memberTarget, memberToken, memberInsecure = oldTarget, oldToken, oldInsecure })
			memberTarget, memberToken, memberInsecure = srv.URL, tc.flag, false
			require.NoError(t, memberDo(http.MethodPost, "/v1/membership", []byte(`{"id":4,"url":"http://n4:9022"}`)))
			setLiveFlags(t, srv.URL, filepath.Join(t.TempDir(), "backup.tar"))
			backupToken = tc.flag
			require.NoError(t, runLiveBackup())
			want := ""
			if tc.want != "" {
				want = "Bearer " + tc.want
			}
			require.Equal(t, want, <-headers, "membership credential")
			require.Equal(t, want, <-headers, "live backup credential")
		})
	}
}
