package http_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The dry-run endpoint is the authoring loop, so its refusals carry the
// parser's own words: a kind that cannot rehearse says so, and a bad
// parameter names its bound. Nothing is admitted on any path.
func TestDryRunSyncable_RefusalsCarryTheReason(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	body := "[syncable]\nname = \"probe\"\ntype = \"recorder\"\n[recorder]\ntopic = \"photos\"\n"

	w := e.doTOML(t, "POST", "/v1/syncable/dryrun", body)
	requireEnvelope(t, w, 400, "invalid_config")
	require.Contains(t, w.Body.String(), "dry-run")

	for _, tc := range []struct{ query, want string }{
		{"?maxEntries=0", "maxEntries"},
		{"?maxEntries=notanumber", "maxEntries"},
		{"?timeoutSeconds=0", "timeoutSeconds"},
		{"?fromIndex=abc", "fromIndex"},
	} {
		w := e.doTOML(t, "POST", "/v1/syncable/dryrun"+tc.query, body)
		requireEnvelope(t, w, 400, "invalid_config")
		require.Contains(t, w.Body.String(), tc.want, tc.query)
	}

	w = e.doEmpty(t, "GET", "/v1/syncable/probe/status")
	mustStatus(t, w, 404)
}
