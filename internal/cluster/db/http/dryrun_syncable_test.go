package http_test

import (
	"strings"
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

	// The rehearsal answers exactly as the real POST would, so a pipeline can
	// reuse its output: same resource-scoped code, same {field, issue} detail.
	// An unknown key in the closed [syncable] envelope — the shared parser
	// rejects it with the field named, which is what must survive the hop.
	bad := "[syncable]\nname = \"probe\"\ntype = \"recorder\"\nbogus = 1\n[recorder]\ntopic = \"photos\"\n"
	w := e.doTOML(t, "POST", "/v1/syncable/dryrun", bad)
	requireEnvelope(t, w, 400, "invalid_syncable_config")
	require.Contains(t, w.Body.String(), `"field"`,
		"the rehearsal must carry the same {field, issue} detail the real POST gives")

	// A kind with no rehearsal is a 409 of its own — the config is valid,
	// there is simply nothing to rehearse. Reporting that as invalid_config
	// told an author their good config was bad.
	w = e.doTOML(t, "POST", "/v1/syncable/dryrun", strings.ReplaceAll(body, "recorder", "nodrop"))
	requireEnvelope(t, w, 409, "dry_run_unsupported")

	for _, tc := range []struct{ query, want string }{
		{"?maxEntries=0", "maxEntries"},
		{"?maxEntries=notanumber", "maxEntries"},
		{"?timeoutSeconds=0", "timeoutSeconds"},
		{"?fromIndex=abc", "fromIndex"},
	} {
		w := e.doTOML(t, "POST", "/v1/syncable/dryrun"+tc.query, body)
		requireEnvelope(t, w, 400, "invalid_parameter")
		require.Contains(t, w.Body.String(), tc.want, tc.query)
	}

	w = e.doEmpty(t, "GET", "/v1/syncable/probe/status")
	mustStatus(t, w, 404)
}
