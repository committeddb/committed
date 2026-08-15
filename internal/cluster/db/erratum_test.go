package db_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	synchttp "github.com/committeddb/committed/internal/cluster/syncable/http"
)

// newWalDBErrata builds the errata test fixture: version-announced (the
// erratum gate requires the cluster minimum feature level) with the webhook
// syncable parser registered (the observable sink).
func newWalDBErrata(t *testing.T) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	p.AddSyncableParser("http", &synchttp.SyncableParser{})
	// Real pump channels: the applied syncable configs must reach
	// listenForSyncables so workers actually start (a nil pump drops them).
	syncCh := make(chan *db.SyncableWithID, 32)
	ingestCh := make(chan *db.IngestableWithID, 32)
	s, err := wal.Open(dir, p, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, ingestCh,
		db.WithTickInterval(testTickInterval), db.WithVersionAnnounce())
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s
}

func proposeErratumTOML(t *testing.T, d *db.DB, id, body string) error {
	t.Helper()
	return d.ProposeErratum(testCtx(t), &cluster.Configuration{
		ID: id, MimeType: "text/toml", Data: []byte(body),
	})
}

func awaitFeatureLevel(t *testing.T, d *db.DB) {
	t.Helper()
	require.Eventually(t, func() bool {
		// The self-announce is async; probe via an erratum that fails LATER
		// admission (unknown type) once the gate opens.
		err := proposeErratumTOML(t, d, "probe",
			"[erratum]\ntype = \"no-such-type\"\nfromIndex = 1\ntoIndex = 1\nrebindToVersion = 1\n")
		var lvl *cluster.ClusterBelowFeatureLevelError
		return !errors.As(err, &lvl)
	}, 10*time.Second, 10*time.Millisecond, "feature level never announced")
}

// TestErratum_AdmissionMatrix pins the loud-at-POST rules and the append-only
// immutability contract.
func TestErratum_AdmissionMatrix(t *testing.T) {
	d, s := newWalDBErrata(t)
	proposeTypeTOML(t, d, "photos", "photos", "", "")
	proposeTypeTOML(t, d, "photos", "photos", `{"type":"object"}`, "\n[migration]\nnone = true\n")
	v2, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	require.Equal(t, 2, v2.Version)
	awaitFeatureLevel(t, d)
	applied := s.AppliedIndex()

	// Unknown type.
	err = proposeErratumTOML(t, d, "e-a", "[erratum]\ntype = \"nope\"\nfromIndex = 1\ntoIndex = 2\nrebindToVersion = 1\n")
	require.ErrorContains(t, err, "not a declared version")

	// Undeclared rebind target / stamp selector.
	err = proposeErratumTOML(t, d, "e-b", "[erratum]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = 2\nrebindToVersion = 9\n")
	require.ErrorContains(t, err, "rebindToVersion 9")
	err = proposeErratumTOML(t, d, "e-c", "[erratum]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = 2\nrebindToVersion = 2\nfromVersion = 9\n")
	require.ErrorContains(t, err, "fromVersion 9")

	// A range beyond the applied log (errata bind the past).
	err = proposeErratumTOML(t, d, "e-d",
		fmt.Sprintf("[erratum]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = %d\nrebindToVersion = 2\n", applied+1000))
	require.ErrorContains(t, err, "beyond the applied log")

	// An inverted range, and a non-deterministic predicate.
	err = proposeErratumTOML(t, d, "e-e", "[erratum]\ntype = \"photos\"\nfromIndex = 5\ntoIndex = 2\nrebindToVersion = 2\n")
	require.ErrorContains(t, err, "must not exceed")
	err = proposeErratumTOML(t, d, "e-f", "[erratum]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = 2\nrebindToVersion = 2\npredicate = \"now\"\n")
	require.ErrorContains(t, err, "deterministic")

	// A valid erratum admits; an identical re-POST is an idempotent no-op; a
	// DIFFERENT re-POST under the same id is refused (append-only).
	good := "[erratum]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = 2\nrebindToVersion = 2\n"
	require.NoError(t, proposeErratumTOML(t, d, "e-good", good))
	require.NoError(t, proposeErratumTOML(t, d, "e-good", good))
	err = proposeErratumTOML(t, d, "e-good", "[erratum]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = 3\nrebindToVersion = 2\n")
	require.ErrorContains(t, err, "append-only")

	// The listing carries it with its interpretation coordinate.
	applied2, err := d.Errata()
	require.NoError(t, err)
	require.Len(t, applied2, 1)
	require.Equal(t, "e-good", applied2[0].Erratum.ID)
	require.NotZero(t, applied2[0].Index)
	require.Equal(t, applied2[0].Index, s.InterpretationRegistry().Highwater())
}

// TestErratum_FeatureGateRefusesUntilAnnounced pins the mixed-version rule:
// a cluster whose minimum feature level predates errata refuses the POST with
// the retryable typed error (the record is gated — an old member would fatal
// applying it).
func TestErratum_FeatureGateRefusesUntilAnnounced(t *testing.T) {
	d, _ := newWalDB(t) // no WithVersionAnnounce: cluster min stays 0
	proposeTypeTOML(t, d, "photos", "photos", "", "")

	err := proposeErratumTOML(t, d, "e-1", "[erratum]\ntype = \"photos\"\nfromIndex = 1\ntoIndex = 1\nrebindToVersion = 1\n")
	var lvl *cluster.ClusterBelowFeatureLevelError
	require.ErrorAs(t, err, &lvl)
	require.Equal(t, uint64(2), lvl.Required)
}

// webhookRecorder records webhook deliveries (the observable sink).
type webhookRecorder struct {
	mu     sync.Mutex
	bodies [][]byte
}

func (wr *webhookRecorder) handler() httpgo.HandlerFunc {
	return func(w httpgo.ResponseWriter, r *httpgo.Request) {
		body, _ := io.ReadAll(r.Body)
		wr.mu.Lock()
		wr.bodies = append(wr.bodies, body)
		wr.mu.Unlock()
		w.WriteHeader(200)
	}
}

// deliveries decodes every recorded upsert as (key → {version, data}).
func (wr *webhookRecorder) deliveries(t *testing.T) map[string]struct {
	Version int
	Data    map[string]any
} {
	t.Helper()
	out := map[string]struct {
		Version int
		Data    map[string]any
	}{}
	wr.mu.Lock()
	defer wr.mu.Unlock()
	for _, b := range wr.bodies {
		var p struct {
			Entities []struct {
				Op   string `json:"op"`
				Key  string `json:"key"`
				Type struct {
					Version int `json:"version"`
				} `json:"type"`
				Data json.RawMessage `json:"data"`
			} `json:"entities"`
		}
		require.NoError(t, json.Unmarshal(b, &p), "webhook body: %s", b)
		for _, e := range p.Entities {
			var data map[string]any
			if len(e.Data) > 0 {
				require.NoError(t, json.Unmarshal(e.Data, &data))
			}
			key, err := base64.StdEncoding.DecodeString(e.Key)
			require.NoError(t, err, "webhook keys are base64")
			out[string(key)] = struct {
				Version int
				Data    map[string]any
			}{e.Type.Version, data}
		}
	}
	return out
}

// TestErratum_RebindsReadingsEndToEnd is the spine's e2e at the db layer —
// the SmugMug repair: v2-shaped rows were committed under v1 stamps (nobody
// announced the change); the operator declares v2 with a migration transform
// and an erratum rebinding the known-v2-shaped range. An always-current
// webhook sink then receives: rebound rows AS v2 (the wrong transform never
// runs on them — that is the repair) and genuine v1 rows migrated through the
// chain — with the log bytes untouched throughout. A second, identical
// consumer replaying from scratch at the same (data, interpretation) pair
// sees identical readings (replay determinism).
func TestErratum_RebindsReadingsEndToEnd(t *testing.T) {
	d, s := newWalDBErrata(t)
	recorder := &webhookRecorder{}
	server := httptest.NewServer(recorder.handler())
	t.Cleanup(server.Close)

	proposeTypeTOML(t, d, "photos", "photos", "", "")
	tp1, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)

	// Four rows under v1 stamps: k1/k4 genuinely v1-shaped, k2/k3 already
	// v2-shaped (the unannounced writer).
	for _, row := range []struct{ k, data string }{
		{"k1", `{"caption":"a"}`},
		{"k2", `{"caption":"b","license":"cc"}`},
		{"k3", `{"caption":"c","license":"arr"}`},
		{"k4", `{"caption":"d"}`},
	} {
		require.NoError(t, d.Propose(testCtx(t),
			&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp1, []byte(row.k), []byte(row.data))}}))
	}

	// Locate each row's raft index (the erratum range coordinates).
	indexByKey := map[string]uint64{}
	r := s.Reader("erratum-verify")
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

	// Declare v2 with a transform that upgrades v1 shapes — a transform that
	// would CORRUPT already-v2 data by running twice (the classic).
	proposeTypeTOML(t, d, "photos", "photos", `{"type":"object"}`,
		"\n[migration]\ntransform = '. + {license: \"unknown\"}'\n")

	awaitFeatureLevel(t, d)

	// The erratum: k2..k3 were v2 all along.
	require.NoError(t, proposeErratumTOML(t, d, "backfill-v2", fmt.Sprintf(
		"[erratum]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nrebindToVersion = 2\nfromVersion = 1\n",
		indexByKey["k2"], indexByKey["k3"])))

	// An always-current webhook sink over the topic.
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "photos-hook", MimeType: "text/toml",
		Data: fmt.Appendf(nil, "[syncable]\nname = \"photos-hook\"\ntype = \"http\"\nmode = \"always-current\"\n\n[http]\ntopic = \"photos\"\nurl = %q\n", server.URL),
	}))

	var got map[string]struct {
		Version int
		Data    map[string]any
	}
	require.Eventually(t, func() bool {
		got = recorder.deliveries(t)
		return len(got) == 4
	}, 15*time.Second, 10*time.Millisecond, "the sink never received all four rows")

	// Rebound rows arrive AS v2, untransformed — their real license survives.
	require.Equal(t, 2, got["k2"].Version)
	require.Equal(t, "cc", got["k2"].Data["license"], "the erratum kept the wrong transform OFF already-v2 data")
	require.Equal(t, "arr", got["k3"].Data["license"])
	// Genuine v1 rows migrate through the chain to v2.
	require.Equal(t, 2, got["k1"].Version)
	require.Equal(t, "unknown", got["k1"].Data["license"], "genuine v1 data migrated")
	require.Equal(t, "unknown", got["k4"].Data["license"])

	// Replay determinism: a second, fresh consumer over the same log and
	// registry converges to the same readings for the original range —
	// including the same rebinds.
	recorder2 := &webhookRecorder{}
	server2 := httptest.NewServer(recorder2.handler())
	t.Cleanup(server2.Close)
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "photos-hook-2", MimeType: "text/toml",
		Data: fmt.Appendf(nil, "[syncable]\nname = \"photos-hook-2\"\ntype = \"http\"\nmode = \"always-current\"\n\n[http]\ntopic = \"photos\"\nurl = %q\n", server2.URL),
	}))
	require.Eventually(t, func() bool {
		return len(recorder2.deliveries(t)) == 4
	}, 15*time.Second, 10*time.Millisecond)
	got2 := recorder2.deliveries(t)
	for _, k := range []string{"k1", "k2", "k3", "k4"} {
		require.Equal(t, got[k], got2[k], "replay from scratch diverged for %s", k)
	}

	// Not stale: this materialization began under the erratum. A LATER
	// erratum flips it stale — loud and queryable, never auto-healed — while
	// the pin stays put.
	pin, stale, err := d.SyncableInterpretation("photos-hook")
	require.NoError(t, err)
	require.False(t, stale)
	require.NotZero(t, pin)
	require.NoError(t, proposeErratumTOML(t, d, "later", fmt.Sprintf(
		"[erratum]\ntype = \"photos\"\nfromIndex = %d\ntoIndex = %d\nrebindToVersion = 2\n",
		indexByKey["k1"], indexByKey["k1"])))
	require.Eventually(t, func() bool {
		pin2, stale2, err := d.SyncableInterpretation("photos-hook")
		return err == nil && stale2 && pin2 == pin
	}, 10*time.Second, 10*time.Millisecond, "a later erratum must mark the syncable stale without moving its pin")
}
