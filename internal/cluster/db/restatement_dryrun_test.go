package db_test

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	synchttp "github.com/committeddb/committed/internal/cluster/syncable/http"
)

// dryRunRestatement rehearses a TOML restatement against the fixture's log.
func dryRunRestatement(t *testing.T, d *db.DB, body string, opts cluster.DryRunOptions) (*cluster.RestatementDryRunReport, error) {
	t.Helper()
	return d.DryRunRestatement(testCtx(t), "text/toml", []byte(body), opts)
}

// seedRestatementDryRunRows builds the shared fixture: type "photos" v1, four
// rows stamped v1 (k2/k3 carry a license field the predicate tests key on),
// then v2 declared. Returns the per-key raft indexes.
func seedRestatementDryRunRows(t *testing.T, d *db.DB, s *wal.Storage) map[string]uint64 {
	t.Helper()
	proposeTypeTOML(t, d, "photos", "photos", "", "")
	tp1, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	for _, row := range []struct{ k, data string }{
		{"k1", `{"caption":"a"}`},
		{"k2", `{"caption":"b","license":"cc"}`},
		{"k3", `{"caption":"c","license":"arr"}`},
		{"k4", `{"caption":"d"}`},
	} {
		require.NoError(t, d.Propose(testCtx(t),
			&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp1, []byte(row.k), []byte(row.data))}}))
	}
	indexByKey := map[string]uint64{}
	r := s.Reader("restatement-dryrun-verify")
	for {
		a, err := r.Read()
		if err != nil {
			break
		}
		for _, e := range a.Entities {
			if e.Type != nil && e.Type.ID == "photos" {
				indexByKey[string(e.Key)] = a.Index
			}
		}
	}
	require.Len(t, indexByKey, 4)
	proposeTypeTOML(t, d, "photos", "photos", `{"type":"object"}`, "\n[migration]\nnone = true\n")
	return indexByKey
}

// TestRestatementDryRun_CensusSamplesAndNoAdmission pins the core rehearsal: the
// census counts what the selectors catch, the samples carry the before/after
// readings, coverage over the restatement's own range reads complete — and
// NOTHING is admitted (the registry stays empty; a re-run is identical).
func TestRestatementDryRun_CensusSamplesAndNoAdmission(t *testing.T) {
	d, s := newWalDBRestatements(t)
	idx := seedRestatementDryRunRows(t, d, s)

	body := fmt.Sprintf("[restatement]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nreadAsVersion = 2\nfromVersion = 1\n",
		idx["k2"], idx["k3"])
	rep, err := dryRunRestatement(t, d, body, cluster.DryRunOptions{})
	require.NoError(t, err)

	require.Equal(t, 2, rep.EntitiesOfType, "two photos rows sit in [k2, k3]")
	require.Equal(t, 2, rep.StampEligible)
	require.Equal(t, 2, rep.Matched)
	require.Equal(t, 2, rep.Rebound, "no prior restatements: both matches change reading v1 -> v2")
	require.Zero(t, rep.PredicateErrors)
	require.Equal(t, map[int]int{1: 2}, rep.ByStampedVersion)
	require.Equal(t, "complete", rep.Coverage)
	require.Empty(t, rep.Truncated)
	require.Len(t, rep.Samples, 2)
	for _, sm := range rep.Samples {
		require.Equal(t, 1, sm.StampedVersion)
		require.Equal(t, 1, sm.CurrentReading)
		require.Equal(t, 2, sm.CandidateReading)
	}

	// The rehearsal admitted nothing.
	applied, err := d.Restatements()
	require.NoError(t, err)
	require.Empty(t, applied, "a dry-run must never admit a restatement")

	// And it is stateless: an identical re-run reports identically.
	rep2, err := dryRunRestatement(t, d, body, cluster.DryRunOptions{})
	require.NoError(t, err)
	require.Equal(t, rep.Matched, rep2.Matched)
	require.Equal(t, rep.Rebound, rep2.Rebound)
}

// TestRestatementDryRun_PredicateNarrowsAndCountsErrors pins the predicate half:
// the census separates stamp-eligible from predicate-matched, and a payload
// the predicate cannot evaluate is counted (with a finding), not fatal.
func TestRestatementDryRun_PredicateNarrowsAndCountsErrors(t *testing.T) {
	d, s := newWalDBRestatements(t)
	idx := seedRestatementDryRunRows(t, d, s)
	// One non-JSON row inside the range extension — the predicate can't read it.
	tp1, err := s.ResolveType(cluster.TypeRefAt("photos", 1))
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp1, []byte("k5"), []byte("not json"))}}))
	to := s.AppliedIndex()

	body := fmt.Sprintf("[restatement]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nreadAsVersion = 2\nfromVersion = 1\npredicate = '.license == \"cc\"'\n",
		idx["k1"], to)
	rep, err := dryRunRestatement(t, d, body, cluster.DryRunOptions{})
	require.NoError(t, err)

	require.Equal(t, 5, rep.StampEligible)
	require.Equal(t, 1, rep.Matched, "only k2 carries license cc")
	require.Equal(t, 1, rep.Rebound)
	require.Equal(t, 1, rep.PredicateErrors, "the non-JSON row fails evaluation, counted not fatal")
	require.Len(t, rep.Samples, 1)
	require.Equal(t, "k2", rep.Samples[0].Key)
	requireFinding(t, rep.Findings, "failed predicate evaluation")
}

// TestRestatementDryRun_AuthoringFindings pins the two headline signatures: an
// restatement that matches nothing, and one that matches but changes no reading.
func TestRestatementDryRun_AuthoringFindings(t *testing.T) {
	d, s := newWalDBRestatements(t)
	idx := seedRestatementDryRunRows(t, d, s)

	// Matches nothing: fromVersion 2 — no row is stamped v2.
	rep, err := dryRunRestatement(t, d, fmt.Sprintf(
		"[restatement]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nreadAsVersion = 1\nfromVersion = 2\n",
		idx["k1"], idx["k4"]), cluster.DryRunOptions{})
	require.NoError(t, err)
	require.Zero(t, rep.Matched)
	requireFinding(t, rep.Findings, "matches NOTHING")

	// A no-op: rebinding v1 stamps to version 1 — every reading already is 1.
	rep, err = dryRunRestatement(t, d, fmt.Sprintf(
		"[restatement]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nreadAsVersion = 1\nfromVersion = 1\n",
		idx["k1"], idx["k4"]), cluster.DryRunOptions{})
	require.NoError(t, err)
	require.Equal(t, 4, rep.Matched)
	require.Zero(t, rep.Rebound)
	requireFinding(t, rep.Findings, "changes NO readings")
}

// TestRestatementDryRun_AdmissionMirror pins that the dry-run refuses exactly
// where the real POST would, with the same words.
func TestRestatementDryRun_AdmissionMirror(t *testing.T) {
	d, s := newWalDBRestatements(t)
	seedRestatementDryRunRows(t, d, s)

	_, err := dryRunRestatement(t, d,
		"[restatement]\ntype = \"nope\"\nfromIndex = 1\ntoIndex = 2\nreadAsVersion = 1\n", cluster.DryRunOptions{})
	require.ErrorContains(t, err, "not a declared version")

	_, err = dryRunRestatement(t, d, fmt.Sprintf(
		"[restatement]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = %d\nreadAsVersion = 1\n", s.AppliedIndex()+100), cluster.DryRunOptions{})
	require.ErrorContains(t, err, "beyond the applied log")

	_, err = dryRunRestatement(t, d,
		"[restatement]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = 2\nreadAsVersion = 1\npredicate = 'now'\n", cluster.DryRunOptions{})
	require.ErrorContains(t, err, "deterministic")
}

// TestRestatementDryRun_ComposesWithAppliedRestatementsAndPreviewsStaleness pins the
// advisory half: an applied restatement shows up as an overlap AND as the current
// reading the candidate composes against (a duplicate rehearsal reports
// itself a no-op); a syncable consuming the topic appears as the
// re-materialization bill.
func TestRestatementDryRun_ComposesWithAppliedRestatementsAndPreviewsStaleness(t *testing.T) {
	d, s := newWalDBRestatements(t)
	idx := seedRestatementDryRunRows(t, d, s)
	awaitFeatureLevel(t, d)

	require.NoError(t, proposeRestatementTOML(t, d, "backfill-v2", fmt.Sprintf(
		"[restatement]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nreadAsVersion = 2\nfromVersion = 1\n",
		idx["k2"], idx["k3"])))

	server := httptest.NewServer(new(webhookRecorder).handler())
	t.Cleanup(server.Close)
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "photos-hook", MimeType: "text/toml",
		Data: fmt.Appendf(nil, "[syncable]\nname = \"photos-hook\"\ntype = \"http\"\n\n[http]\ntopic = \"photos\"\nurl = %q\n", server.URL),
	}))

	// Rehearse the SAME rebind again: it overlaps the applied restatement and —
	// because the fold composes — changes nothing anymore.
	rep, err := dryRunRestatement(t, d, fmt.Sprintf(
		"[restatement]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nreadAsVersion = 2\nfromVersion = 1\n",
		idx["k2"], idx["k3"]), cluster.DryRunOptions{})
	require.NoError(t, err)
	require.Equal(t, 2, rep.Matched)
	require.Zero(t, rep.Rebound, "the applied restatement already gives these rows reading 2")
	require.Contains(t, rep.Overlaps, "backfill-v2")
	requireFinding(t, rep.Findings, "changes NO readings")

	ids := make([]string, 0, len(rep.AffectedSyncables))
	for _, as := range rep.AffectedSyncables {
		ids = append(ids, as.ID)
	}
	require.Contains(t, ids, "photos-hook", "the topic's consumer is the re-materialization bill")
	requireFinding(t, rep.Findings, "syncable(s) stale")
}

// TestRestatementDryRun_BudgetTruncatesHonestly pins the partial-coverage
// posture: an exhausted budget yields a partial report that says so, never a
// silently-complete-looking one.
func TestRestatementDryRun_BudgetTruncatesHonestly(t *testing.T) {
	d, s := newWalDBRestatements(t)
	idx := seedRestatementDryRunRows(t, d, s)

	rep, err := dryRunRestatement(t, d, fmt.Sprintf(
		"[restatement]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nreadAsVersion = 2\nfromVersion = 1\n",
		idx["k1"], idx["k4"]), cluster.DryRunOptions{MaxEntries: 1})
	require.NoError(t, err)
	require.Equal(t, "partial", rep.Coverage)
	require.Contains(t, rep.Truncated, "budget")
}

// TestRestatementDryRun_BelowFeatureLevelStillRehearses pins the gate posture: a
// cluster that cannot yet ADMIT restatements can still rehearse one (the dry-run
// admits nothing), and the report says the real POST would be refused.
func TestRestatementDryRun_BelowFeatureLevelStillRehearses(t *testing.T) {
	// No version announce: the cluster minimum stays 0.
	dir := t.TempDir()
	p := parser.New()
	p.AddSyncableParser("http", &synchttp.SyncableParser{})
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })

	idx := seedRestatementDryRunRows(t, d, s)
	rep, err := dryRunRestatement(t, d, fmt.Sprintf(
		"[restatement]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nreadAsVersion = 2\nfromVersion = 1\n",
		idx["k2"], idx["k3"]), cluster.DryRunOptions{})
	require.NoError(t, err, "rehearsal must work below the feature level — it admits nothing")
	require.Equal(t, 2, rep.Matched)
	requireFinding(t, rep.Findings, "would currently be refused")
}

// requireFinding asserts some finding contains the fragment.
func requireFinding(t *testing.T, findings []string, fragment string) {
	t.Helper()
	for _, f := range findings {
		if strings.Contains(f, fragment) {
			return
		}
	}
	t.Fatalf("no finding contains %q; findings: %v", fragment, findings)
}
