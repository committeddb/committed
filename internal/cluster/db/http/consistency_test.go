package http_test

import (
	"testing"
)

// Route-level smoke for the read-consistency contract against the real
// engine: the default (linearizable) and ?consistency=stale reads both
// serve on a healthy node, and a junk value is rejected. The barrier's full
// contract — read-index counted, 503 on quorum failure, the 400 leg's
// never-reads guarantee — is pinned as a unit in consistency_barrier_test.go
// (a healthy single node cannot fail its own read-index).
func TestConsistency_RouteSmoke(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")

	mustStatus(t, e.doEmpty(t, "GET", "/v1/type"), 200)
	mustStatus(t, e.doEmpty(t, "GET", "/v1/type?consistency=stale"), 200)
	mustStatus(t, e.doEmpty(t, "GET", "/v1/type/photos/versions?consistency=stale"), 200)
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/type?consistency=eventually"), 400, "invalid_consistency")
}
