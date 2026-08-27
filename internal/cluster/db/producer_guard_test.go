package db_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// topicIngestParser is a test ingestable kind that reports the topic it
// produces (cluster.IngestableTopicExtractor) and builds an idle worker —
// the minimal shape the producer guard needs from a real ingest kind.
type topicIngestParser struct{}

func (p *topicIngestParser) Parse(*cluster.ParsedConfig) (cluster.Ingestable, error) {
	return reconcileFakeIngestable{}, nil
}

func (p *topicIngestParser) TopicsFromConfig(v *cluster.ParsedConfig) []string {
	if t := v.GetString("fake.topic"); t != "" {
		return []string{t}
	}
	return v.GetStringSlice("fake.topics")
}

func proposeFakeIngest(t *testing.T, d *db.DB, id, topic string) error {
	t.Helper()
	data := fmt.Sprintf("[ingestable]\nname = %q\ntype = \"fake\"\n[fake]\ntopic = %q\n", id, topic)
	return d.ProposeIngestable(testCtx(t),
		&cluster.Configuration{ID: id, MimeType: "text/toml", Data: []byte(data)})
}

// TestProducerGuard_SecondIngestableRefused pins the consensus-authoritative
// half of single-producer-per-topic for the ingestable×ingestable direction:
// the second claim on a topic is refused at POST, naming the holder; a
// re-POST of the same id is the same authority, not a second one.
func TestProducerGuard_SecondIngestableRefused(t *testing.T) {
	sink := &recorderSink{}
	d, _ := newWalDBLoopback(t, sink)
	d.AddIngestableParser("fake", &topicIngestParser{})
	proposeKindedType(t, d, "t1", "snapshot")

	require.NoError(t, proposeFakeIngest(t, d, "a", "t1"))

	err := proposeFakeIngest(t, d, "b", "t1")
	require.ErrorContains(t, err, "already has a producer", "a second epoch-stamping producer is refused")
	require.ErrorContains(t, err, `ingestable "a"`, "the refusal names the colliding config")

	require.NoError(t, proposeFakeIngest(t, d, "a", "t1"), "a same-id re-POST is the same authority")
}

// TestProducerGuard_LoopbackAndIngestableCollide pins the cross-kind
// directions: an ingestable may not claim a loopback's derived topic, and a
// loopback may not derive into an ingestable's topic — both refusals name
// the colliding config and its kind.
func TestProducerGuard_LoopbackAndIngestableCollide(t *testing.T) {
	sink := &recorderSink{}
	d, _ := newWalDBLoopback(t, sink)
	d.AddIngestableParser("fake", &topicIngestParser{})
	for _, id := range []string{"a", "b", "x"} {
		proposeKindedType(t, d, id, "snapshot")
	}

	require.NoError(t, proposeLoopback(t, d, "ab", "a", "b", ""))
	err := proposeFakeIngest(t, d, "ing-b", "b")
	require.ErrorContains(t, err, "already has a producer")
	require.ErrorContains(t, err, `syncable "ab"`, "the refusal names the loopback holding the topic")

	require.NoError(t, proposeFakeIngest(t, d, "ing-x", "x"))
	err = proposeLoopback(t, d, "into-x", "a", "x", "")
	require.ErrorContains(t, err, "already has a producer")
	require.ErrorContains(t, err, `ingestable "ing-x"`, "the refusal names the ingestable holding the topic")
}

// TestProducerGuard_RaceCommittedDegradesDeterministically: a config that
// slips PAST the leader's admission check (raced proposes; simulated by
// committing the raw config entity) is refused at APPLY by the joint
// log-index-order replay: persisted, loudly degraded, never started — so
// two epoch-stamping producers can never actually run on one topic. The
// earlier config keeps its worker.
func TestProducerGuard_RaceCommittedDegradesDeterministically(t *testing.T) {
	sink := &recorderSink{}
	d, _ := newWalDBLoopback(t, sink)
	d.AddIngestableParser("fake", &topicIngestParser{})
	proposeKindedType(t, d, "t1", "snapshot")

	require.NoError(t, proposeFakeIngest(t, d, "win", "t1"))
	require.Eventually(t, func() bool { return d.HasIngestWorkerForTest("win") },
		10*time.Second, 10*time.Millisecond, "the winner's worker must start")

	// Bypass admission: commit the colliding config as a raw entity.
	cfg := &cluster.Configuration{
		ID: "lose", MimeType: "text/toml",
		Data: []byte("[ingestable]\nname = \"lose\"\ntype = \"fake\"\n[fake]\ntopic = \"t1\"\n"),
	}
	e, err := cluster.NewUpsertIngestableEntity(cfg)
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))

	require.Eventually(t, func() bool {
		for _, ce := range d.ConfigBuildErrors() {
			if ce.Kind == "ingestable" && ce.ID == "lose" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "the raced-in producer never degraded")

	require.False(t, d.HasIngestWorkerForTest("lose"), "the loser must never start a worker")
	require.True(t, d.HasIngestWorkerForTest("win"), "the winner must keep its worker")
	for _, ce := range d.ConfigBuildErrors() {
		require.NotEqual(t, "win", ce.ID, "the earlier config must stay accepted")
	}
}

// TestProducerGuard_RacedIngestableLosesToEarlierLoopback covers the raced
// cross-kind window: an ingestable committed after a loopback already
// produces its topic degrades deterministically, kind-tagged.
func TestProducerGuard_RacedIngestableLosesToEarlierLoopback(t *testing.T) {
	sink := &recorderSink{}
	d, _ := newWalDBLoopback(t, sink)
	d.AddIngestableParser("fake", &topicIngestParser{})
	proposeKindedType(t, d, "a", "snapshot")
	proposeKindedType(t, d, "b", "snapshot")
	require.NoError(t, proposeLoopback(t, d, "ab", "a", "b", ""))

	cfg := &cluster.Configuration{
		ID: "ing-lose", MimeType: "text/toml",
		Data: []byte("[ingestable]\nname = \"ing-lose\"\ntype = \"fake\"\n[fake]\ntopic = \"b\"\n"),
	}
	e, err := cluster.NewUpsertIngestableEntity(cfg)
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))

	require.Eventually(t, func() bool {
		for _, ce := range d.ConfigBuildErrors() {
			if ce.Kind == "ingestable" && ce.ID == "ing-lose" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "the raced-in cross-kind producer never degraded")
	require.False(t, d.HasIngestWorkerForTest("ing-lose"))
}

// TestReplayDerivation_MixedKinds pins the joint replay's identity and
// ordering semantics across kinds: first-by-log-index wins regardless of
// kind, the two namespaces may reuse an ID without merging, and deleting
// the winner (removing its edge) deterministically un-refuses the loser.
func TestReplayDerivation_MixedKinds(t *testing.T) {
	ing := db.DerivationEdge{Kind: "ingestable", ID: "x", Index: 5, Targets: []string{"t"}}
	loop := db.DerivationEdge{Kind: "syncable", ID: "x", Index: 9, Sources: []string{"s"}, Targets: []string{"t"}}

	refused := db.ReplayDerivation([]db.DerivationEdge{ing, loop})
	require.Len(t, refused, 1)
	err := refused[db.EdgeRef{Kind: "syncable", ID: "x"}]
	require.Error(t, err, "the later-indexed config loses, even sharing the winner's ID across namespaces")
	require.ErrorContains(t, err, `ingestable "x"`, "the refusal names the winner's kind")

	// Swap the order: the ingestable landed later and loses.
	ing.Index, loop.Index = 9, 5
	refused = db.ReplayDerivation([]db.DerivationEdge{ing, loop})
	require.Len(t, refused, 1)
	require.ErrorContains(t, refused[db.EdgeRef{Kind: "ingestable", ID: "x"}], `syncable "x"`)

	// Winner deleted: its edge is gone from the stored set, so the replay —
	// a pure function of that set — accepts the survivor on every node.
	refused = db.ReplayDerivation([]db.DerivationEdge{ing})
	require.Empty(t, refused, "removing the winner's edge promotes the loser deterministically")
}

// TestProducerGuard_MultiTopicClaimsAllChecked: a multi-topic producer holds
// EVERY topic it claims, and a candidate claiming several topics is refused
// if ANY is taken — the coverage the removed HTTP-layer guard carried, now
// at the consensus-authoritative level.
func TestProducerGuard_MultiTopicClaimsAllChecked(t *testing.T) {
	sink := &recorderSink{}
	d, _ := newWalDBLoopback(t, sink)
	d.AddIngestableParser("fake", &topicIngestParser{})
	for _, id := range []string{"t1", "t2", "t3"} {
		proposeKindedType(t, d, id, "snapshot")
	}

	require.NoError(t, d.ProposeIngestable(testCtx(t), &cluster.Configuration{
		ID: "multi", MimeType: "text/toml",
		Data: []byte("[ingestable]\nname = \"multi\"\ntype = \"fake\"\n[fake]\ntopics = [\"t1\", \"t2\"]\n"),
	}))

	err := proposeFakeIngest(t, d, "second", "t2")
	require.ErrorContains(t, err, "already has a producer", "every topic of a multi-topic producer is held")
	require.ErrorContains(t, err, `ingestable "multi"`)

	err = d.ProposeIngestable(testCtx(t), &cluster.Configuration{
		ID: "overlap", MimeType: "text/toml",
		Data: []byte("[ingestable]\nname = \"overlap\"\ntype = \"fake\"\n[fake]\ntopics = [\"t3\", \"t1\"]\n"),
	})
	require.ErrorContains(t, err, "already has a producer", "a candidate is refused if ANY claimed topic is taken")

	require.NoError(t, proposeFakeIngest(t, d, "third", "t3"), "an untaken topic is free")
}

// TestProducerGuard_WinnerRepostBlockedWhileLoserLingers pins the coupling
// that keeps the current-version-index ordering safe (see
// ReplayWithCandidate): admission replays against the STORED set, refused
// losers' claims included, so re-POSTing the running winner while a
// degraded loser lingers is refused — the flip where the loser (earlier
// index) would take the topic from the freshly-re-indexed winner can never
// commit. Deleting the loser unblocks the re-POST.
func TestProducerGuard_WinnerRepostBlockedWhileLoserLingers(t *testing.T) {
	sink := &recorderSink{}
	d, _ := newWalDBLoopback(t, sink)
	d.AddIngestableParser("fake", &topicIngestParser{})
	proposeKindedType(t, d, "t1", "snapshot")

	require.NoError(t, proposeFakeIngest(t, d, "win", "t1"))

	// Race a loser past admission (raw commit) — it degrades.
	cfg := &cluster.Configuration{
		ID: "lose", MimeType: "text/toml",
		Data: []byte("[ingestable]\nname = \"lose\"\ntype = \"fake\"\n[fake]\ntopic = \"t1\"\n"),
	}
	e, err := cluster.NewUpsertIngestableEntity(cfg)
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))
	require.Eventually(t, func() bool {
		for _, ce := range d.ConfigBuildErrors() {
			if ce.Kind == "ingestable" && ce.ID == "lose" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)

	// The winner's re-POST must be refused while the loser's claim lingers:
	// committing it would make the winner the YOUNGEST claimant and hand the
	// topic to the loser at the next replay.
	err = proposeFakeIngest(t, d, "win", "t1")
	require.ErrorContains(t, err, "already has a producer")
	require.ErrorContains(t, err, `ingestable "lose"`, "the refusal names the lingering loser to delete")

	// Cleanup unblocks: delete the loser, and the winner re-configures freely.
	require.NoError(t, d.DeleteIngestable(testCtx(t), "lose"))
	require.NoError(t, proposeFakeIngest(t, d, "win", "t1"))
}
