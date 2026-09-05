package http_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Migration dead-letter endpoints against the real engine. Producing a real
// migration dead letter means a jq chain failing mid-sync — a journey worth
// its own harness (noted in the retirement ticket); until then the listing
// rendering and the replay outcome mapping are pinned in-package
// (type_mapping_test.go), and everything reachable on a clean engine runs
// real here.

// TestGetTypeMigrationErrors_Defaults: no failures recorded serializes as
// [] (not null), with default paging.
func TestGetTypeMigrationErrors_Defaults(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")

	w := e.doEmpty(t, "GET", "/v1/type/photos/migration-errors")
	mustStatus(t, w, 200)
	require.Equal(t, "[]", w.Body.String())
}

// TestGetTypeMigrationErrors_BadParams: invalid cursor params are 400s.
func TestGetTypeMigrationErrors_BadParams(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	for _, path := range []string{
		"/v1/type/photos/migration-errors?since=notanumber",
		"/v1/type/photos/migration-errors?limit=0",
		"/v1/type/photos/migration-errors?limit=-3",
		"/v1/type/photos/migration-errors?limit=abc",
	} {
		t.Run(path, func(t *testing.T) {
			requireEnvelope(t, e.doEmpty(t, "GET", path), 400, "invalid_parameter")
		})
	}
}

// TestReplayTypeMigrationDeadLetter_NotDeadLettered: retrying an index that
// is not a migration dead letter 404s through the real engine.
func TestReplayTypeMigrationDeadLetter_NotDeadLettered(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/type/photos/migration-retry/7"), 404, "not_dead_lettered")
}

// TestReplayTypeMigrationDeadLetter_BadIndex rejects a non-numeric index.
func TestReplayTypeMigrationDeadLetter_BadIndex(t *testing.T) {
	e := newEngine(t)
	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/type/photos/migration-retry/notanumber"), 400, "invalid_parameter")
}
