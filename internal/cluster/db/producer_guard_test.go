package db_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/wal"
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

// proposeGenRow commits one generation-stamped row on topic — the residue an
// earlier producer era leaves in the log (and on downstream sinks). The apply
// path bumps the topic's refresh-epoch highwater from ANY committed
// generation, which is exactly what the epoch-regression guard reads.
func proposeGenRow(t *testing.T, d *db.DB, s *wal.Storage, topic string, gen uint64) {
	t.Helper()
	tp, err := s.ResolveType(cluster.LatestTypeRef(topic))
	require.NoError(t, err)
	e := cluster.NewUpsertEntity(tp, []byte("k1"), []byte(`{"v":1}`))
	e.Generation = gen
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))
}

// TestProducerGuard_EpochRegressionRefused pins the producer-HANDOVER
// continuity guard: a loopback may not derive into a topic whose committed
// refresh-epoch highwater exceeds its source's. The loopback forwards source
// epochs verbatim, so a lagging source's sweeps could never reconcile the
// previous producer's higher-stamped rows — they would linger stale on every
// downstream keyed sink. A source at or above the target's highwater is
// admitted (its first refresh reconciles the handover completely).
func TestProducerGuard_EpochRegressionRefused(t *testing.T) {
	sink := &recorderSink{}
	d, s := newWalDBLoopback(t, sink)
	proposeKindedType(t, d, "src", "snapshot")
	proposeKindedType(t, d, "tgt", "snapshot")

	// The old producer era: tgt carries committed epochs up to 7; the
	// candidate source has only reached 3.
	proposeGenRow(t, d, s, "tgt", 7)
	proposeGenRow(t, d, s, "src", 3)

	err := proposeLoopback(t, d, "st", "src", "tgt", "")
	require.ErrorContains(t, err, "refresh epochs up to 7", "the refusal names the target's highwater")
	require.ErrorContains(t, err, `source topic "src"`, "the refusal names the lagging source")
	require.ErrorContains(t, err, "fresh topic", "the refusal steers to the safe remedy")

	// The source catching up (or being ahead) is the safe direction: the
	// first forwarded refresh re-emits everything live and its sweep
	// reconciles the handover.
	proposeGenRow(t, d, s, "src", 7)
	require.NoError(t, proposeLoopback(t, d, "st", "src", "tgt", ""),
		"a source at the target's highwater is admitted")
}

// TestProducerGuard_RacedEpochRegressionDegradesThenHeals covers the
// build-time half of the epoch-continuity guard: a lagging-source loopback
// that raced past admission (raw-committed) degrades loudly instead of
// silently stranding the old producer's rows — and self-heals on the
// degraded-build retry once the source's epoch space catches up (the
// handover has become reconcilable).
func TestProducerGuard_RacedEpochRegressionDegradesThenHeals(t *testing.T) {
	sink := &recorderSink{}
	d, s := newWalDBLoopback(t, sink)
	proposeKindedType(t, d, "src", "snapshot")
	proposeKindedType(t, d, "tgt", "snapshot")
	proposeGenRow(t, d, s, "tgt", 7)
	proposeGenRow(t, d, s, "src", 3)

	// Bypass admission: commit the lagging-source loopback as a raw entity.
	cfg := &cluster.Configuration{
		ID: "st", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"st\"\ntype = \"loopback\"\n[loopback]\ntopic = \"src\"\ntarget = \"tgt\"\n"),
	}
	e, err := cluster.NewUpsertSyncableEntity(cfg)
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))

	require.Eventually(t, func() bool {
		for _, ce := range d.ConfigBuildErrors() {
			if ce.Kind == "syncable" && ce.ID == "st" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "the raced-in lagging-source loopback never degraded")
	require.False(t, d.HasSyncWorkerForTest("st"), "a degraded handover must not forward")

	// The source catches up past the old highwater; the next rebuild pass
	// (here the reconcile path — the same build body the minute retry runs)
	// admits.
	proposeGenRow(t, d, s, "src", 8)
	s.RequestSyncReconcile()
	require.Eventually(t, func() bool {
		if !d.HasSyncWorkerForTest("st") {
			return false
		}
		for _, ce := range d.ConfigBuildErrors() {
			if ce.Kind == "syncable" && ce.ID == "st" {
				return false // stale evidence must clear on the healed build
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond, "the handover must self-heal once the source epoch space catches up")
}

// TestDerivedTopicEpochRegression_MinOverSources pins the conservative
// multi-source semantics: one lagging source among several is enough to
// strand rows, so the guard compares against the MINIMUM source highwater.
func TestDerivedTopicEpochRegression_MinOverSources(t *testing.T) {
	epochs := map[string]uint64{"s-ahead": 9, "s-lagging": 3, "tgt": 7}
	epochOf := func(topic string) uint64 { return epochs[topic] }

	err := db.DerivedTopicEpochRegression([]string{"s-ahead", "s-lagging"}, []string{"tgt"}, epochOf)
	require.Error(t, err, "one lagging source among several must refuse")
	require.ErrorContains(t, err, "refresh epochs up to 7")

	require.NoError(t, db.DerivedTopicEpochRegression([]string{"s-ahead"}, []string{"tgt"}, epochOf))
	require.NoError(t, db.DerivedTopicEpochRegression([]string{"s-lagging"}, []string{"fresh"}, epochOf),
		"a target with no epoch history is free")
}
