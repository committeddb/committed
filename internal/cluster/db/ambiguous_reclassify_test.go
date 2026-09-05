package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// siteModelSink models one ambiguous extraction site exactly the way the real
// sinks wire it (sql mapping paths, projection paths, the migration chain): a
// cluster.AmbiguityTracker classifies each failure of the site from the
// site's own history. Three row classes drive the tests: evalFail — the site
// is evaluated and misses (tracker classifies); evalOK — the site is
// evaluated and matches (tracker resets); neither — the site is not
// evaluated at all (a row the rule's when-clause skips: plain success that
// must NOT touch the tracker). plainFail rows fail unmarked Permanent.
type siteModelSink struct {
	mu        sync.Mutex
	tracker   *cluster.AmbiguityTracker
	evalFail  map[string]bool
	evalOK    map[string]bool
	plainFail map[string]bool
	synced    []string
	attempts  map[string]int
}

func newSiteModelSink() *siteModelSink {
	return &siteModelSink{
		tracker:  cluster.NewAmbiguityTracker(),
		evalFail: map[string]bool{}, evalOK: map[string]bool{}, plainFail: map[string]bool{},
		attempts: map[string]int{},
	}
}

func (f *siteModelSink) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range a.Entities {
		if e.Type == nil || cluster.IsInternal(e.Type.ID) || e.Variant() != cluster.EntityVariantRow {
			continue
		}
		k := string(e.Key)
		f.attempts[k]++
		switch {
		case f.evalFail[k]:
			return false, f.tracker.Classify(a.Index, fmt.Errorf("jsonpath [$.typo]: unknown key typo (row %s)", k))
		case f.plainFail[k]:
			return false, cluster.Permanent(fmt.Errorf("entry-specific rejection (row %s)", k))
		case f.evalOK[k]:
			f.tracker.Succeeded()
		}
		f.synced = append(f.synced, k)
	}
	return true, nil
}
func (f *siteModelSink) Close() error { return nil }

func (f *siteModelSink) attemptsFor(k string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[k]
}

func (f *siteModelSink) syncedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.synced)
}

func newWalDBAmbiguous(t *testing.T, sink cluster.Syncable) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	fakeParser := &clusterfakes.FakeSyncableParser{}
	fakeParser.ParseReturns(sink, nil)
	p.AddSyncableParser("fake", fakeParser)
	syncCh := make(chan *db.SyncableWithID, 32)
	ingestCh := make(chan *db.IngestableWithID, 32)
	s, err := wal.Open(dir, p, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, ingestCh,
		db.WithTickInterval(testTickInterval), db.WithSyncStuckThreshold(100*time.Millisecond))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s
}

func seedAmbiguousFixture(t *testing.T, d *db.DB, s *wal.Storage, rows int) *cluster.Type {
	t.Helper()
	proposeTypeTOML(t, d, "photos", "photos", "", "")
	tp, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "sink", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"sink\"\ntype = \"fake\"\n"),
	}))
	for i := 1; i <= rows; i++ {
		require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tp, fmt.Appendf(nil, "k%02d", i), []byte(`{"v":1}`)),
		}}))
	}
	return tp
}

// TestConfigShapedRun_WedgesAtThreshold pins the worker-visible contract of
// site-level classification: a site failing every distinct row dead-letters
// the first threshold-1 rows (bounded, replayable after the fix), then the
// threshold row comes back config-shaped (transient) — the worker WEDGES
// there, retrying visibly, and nothing past the wedge is touched.
func TestConfigShapedRun_WedgesAtThreshold(t *testing.T) {
	sink := newSiteModelSink()
	for i := 1; i <= 20; i++ {
		sink.evalFail[fmt.Sprintf("k%02d", i)] = true
	}
	d, s := newWalDBAmbiguous(t, sink)
	seedAmbiguousFixture(t, d, s, 20)

	// The wedge: the 10th distinct failing row is retried transiently, so its
	// attempt count grows while rows past it are never attempted.
	require.Eventually(t, func() bool {
		return sink.attemptsFor("k10") >= 3
	}, 20*time.Second, 10*time.Millisecond, "the run never wedged on the threshold row")
	require.Zero(t, sink.attemptsFor("k11"), "rows past the wedge must not be attempted")

	// Exactly the pre-threshold rows dead-lettered.
	count, _, _, err := d.SyncableDeadLetterStats("sink")
	require.NoError(t, err)
	require.Equal(t, uint64(9), count, "only the rows before the evidence threshold dead-letter")

	// The worker reports stuck (transient wedge), not parked.
	require.Eventually(t, func() bool {
		stuck, ok, serr := d.SyncableStuck("sink")
		return serr == nil && ok && !stuck.Parked
	}, 20*time.Second, 10*time.Millisecond, "the config-shaped run must surface as a stuck (transient) worker")
}

// TestIsolatedMiss_StillDeadLettersEntrySpecific pins the other half of the
// contract: a site that matches most rows and misses scattered ones (a field
// genuinely absent in those rows) dead-letters just those rows and the topic
// flows on — each success resets the site's evidence, so scattered misses
// never wedge.
func TestIsolatedMiss_StillDeadLettersEntrySpecific(t *testing.T) {
	sink := newSiteModelSink()
	for i := 1; i <= 18; i++ {
		k := fmt.Sprintf("k%02d", i)
		if i%3 == 0 {
			sink.evalFail[k] = true
		} else {
			sink.evalOK[k] = true
		}
	}
	d, s := newWalDBAmbiguous(t, sink)
	seedAmbiguousFixture(t, d, s, 18)

	require.Eventually(t, func() bool {
		return sink.syncedCount() >= 12 // the 12 passing rows
	}, 20*time.Second, 10*time.Millisecond, "the topic must keep flowing around isolated misses")
	// The final miss (k18) trails the 12th success, so poll the count too.
	require.Eventually(t, func() bool {
		count, _, _, err := d.SyncableDeadLetterStats("sink")
		return err == nil && count == 6
	}, 20*time.Second, 10*time.Millisecond, "each isolated miss dead-letters exactly once")
	_, stuckNow, err := d.SyncableStuck("sink")
	require.NoError(t, err)
	require.False(t, stuckNow, "isolated misses must never wedge the worker")
}

// TestWhenGatedTypo_StillWedges pins the gap the site-level rebuild closed: a
// typo inside a when-gated rule on a mixed topic. The gated rows (every 2nd)
// all fail the site; the other rows sync fine WITHOUT evaluating it. A
// worker-level consecutive-run signal would reset on every interleaved
// success and dead-letter every gated row toward the breaker park — the
// per-site run doesn't, so the 10th distinct gated row wedges exactly like
// an ungated run.
func TestWhenGatedTypo_StillWedges(t *testing.T) {
	sink := newSiteModelSink()
	for i := 2; i <= 24; i += 2 {
		sink.evalFail[fmt.Sprintf("k%02d", i)] = true // gated rows: site evaluated, misses
	}
	// Odd rows: the rule's when-clause skips them — plain success, tracker
	// untouched (neither evalFail nor evalOK).
	d, s := newWalDBAmbiguous(t, sink)
	seedAmbiguousFixture(t, d, s, 24)

	// The 10th distinct gated row is k20 — the worker wedges there despite
	// the interleaved successes.
	require.Eventually(t, func() bool {
		return sink.attemptsFor("k20") >= 3
	}, 20*time.Second, 10*time.Millisecond, "interleaved successes must not mask a site that never succeeds")
	require.Zero(t, sink.attemptsFor("k21"), "rows past the wedge must not be attempted")

	count, _, _, err := d.SyncableDeadLetterStats("sink")
	require.NoError(t, err)
	require.Equal(t, uint64(9), count, "only the gated rows before the threshold dead-letter")

	require.Eventually(t, func() bool {
		stuck, ok, serr := d.SyncableStuck("sink")
		return serr == nil && ok && !stuck.Parked
	}, 20*time.Second, 10*time.Millisecond, "the gated config-shaped run must surface as stuck")
}

// TestPlainPermanentRun_IsNotReclassified: an UNMARKED permanent run (a sink
// that deterministically rejects each entry for entry-specific reasons,
// carrying no ambiguity site) keeps dead-lettering per row toward the
// breaker's own (much larger) park threshold.
func TestPlainPermanentRun_IsNotReclassified(t *testing.T) {
	sink := newSiteModelSink()
	for i := 1; i <= 15; i++ {
		sink.plainFail[fmt.Sprintf("k%02d", i)] = true
	}
	d, s := newWalDBAmbiguous(t, sink)
	seedAmbiguousFixture(t, d, s, 16) // one passing row at the end

	require.Eventually(t, func() bool {
		return sink.syncedCount() >= 1
	}, 20*time.Second, 10*time.Millisecond, "unmarked permanents must keep dead-lettering past ten")
	count, _, _, err := d.SyncableDeadLetterStats("sink")
	require.NoError(t, err)
	require.Equal(t, uint64(15), count)
}
