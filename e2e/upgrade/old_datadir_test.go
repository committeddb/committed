//go:build upgrade

// old_datadir_test.go closes the coverage gap the restart test's package
// comment concedes: the old-binary→new-binary READ contract was asserted by
// documentation, never exercised. Each fixture under testdata/ is a data dir
// genuinely written by a released binary (see testdata/capture-fixture.sh —
// synthesizing "old" bytes with the current clusterpb package can never
// reproduce a removed field or a legacy encoding). The CURRENT binary must:
//
//  1. boot over it and serve /ready (open the old WAL, bbolt, raft state);
//  2. replay the WHOLE old log through a syncable (a webhook sink receives
//     every seeded entity — decode of every era's bytes, upserts and the
//     delete tombstone alike);
//  3. run a scrub over the old bytes (the manual POST /v1/scrub) — proven by
//     a SECOND syncable whose fresh replay no longer sees the erased
//     subject's upsert while the survivors still arrive;
//  4. restart cleanly over the now-rewritten dir.
package upgrade_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fixtureEra is one checked-in old-binary data dir and what its replay must
// deliver. Add a new era by running capture-fixture.sh at the released tag
// and appending it here (see testdata/README.md).
type fixtureEra struct {
	name        string
	cdc         bool     // capture with --cdc (MySQL-backed ingest seed; needs docker)
	upserts     []string // every key the old log's replay must deliver
	deletes     []string // every delete tombstone it must deliver
	wantRefresh bool     // CDC-seeded eras carry a refresh-boundary marker
}

var fixtureEras = []fixtureEra{
	{
		// The data-dir support floor: the first envelope-era release. (The
		// flat-encoding era before it — 0.7.2-beta — never had a deployment
		// and is below the floor; its bytes now fail decode by design.)
		name:    "v0.7.3-beta",
		upserts: []string{"mv1", "mv2", "mv3", "mv4"},
		deletes: []string{"subject-erased"},
	},
	{
		// The last pre-0.8.0 era — the upgrade path into this release —
		// CDC-seeded, so it also pins the ingest-written byte surface: the
		// refresh-boundary marker, generation-stamped rows, and
		// SourceSeq/IngestableID-stamped proposals (plus old-regime ingest
		// bookkeeping in bbolt).
		name:        "v0.7.10-beta",
		cdc:         true,
		upserts:     []string{"mv1", "mv2", "mv3", "mv4", "r1", "r2", "r3", "r4"},
		deletes:     []string{"subject-erased"},
		wantRefresh: true,
	},
}

func TestOldDataDir_CurrentBinaryReadsEveryEra(t *testing.T) {
	bin := buildBinary(t)
	for _, era := range fixtureEras {
		t.Run(era.name, func(t *testing.T) {
			runOldDataDirEra(t, bin, era)
		})
	}
}

func runOldDataDirEra(t *testing.T, bin string, era fixtureEra) {
	src := filepath.Join(projectRoot(t), "e2e", "upgrade", "testdata", era.name, "datadir")
	if _, err := os.Stat(src); err != nil {
		// Fixture data dirs are gitignored, never committed: capture one now
		// from the pinned release tag (real old-binary bytes, generated on
		// demand and cached at src for later runs — see
		// capture_fixture_test.go). Needs the release tags (`git fetch
		// --tags` on a shallow clone) and, for a CDC-seeded era, docker at
		// capture time.
		captureFixture(t, era)
	}
	// Work on a copy — the checked-in fixture bytes are the contract and must
	// never be mutated by a run.
	dataDir := filepath.Join(t.TempDir(), "datadir")
	copyDir(t, src, dataDir)

	sink := newWebhookSink()
	defer sink.srv.Close()

	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	// 1. The current binary boots over the old-binary-written dir.
	node := startNode(t, bin, dataDir, port)
	t.Cleanup(func() { node.stopGraceful(t, base) })
	waitReady(t, base)

	// 2. Webhook syncables bootstrap-replay the WHOLE old log: every seeded
	// entity decodes and arrives — upserts, the RTBF delete tombstone, and
	// (for a CDC-seeded era) the refresh-boundary marker.
	postWebhookSyncable(t, base, "era-replay", "movie", sink.srv.URL)
	if era.wantRefresh {
		postWebhookSyncable(t, base, "era-replay-cdc", "cdcrow", sink.srv.URL)
	}
	sink.awaitKeys(t, "upsert", era.upserts...)
	sink.awaitKeys(t, "delete", era.deletes...)
	if era.wantRefresh {
		// A refresh-boundary marker carries no key; op alone identifies it.
		sink.awaitKeys(t, "refresh", "")
	}

	// 3. A scrub over the old bytes: rewrite the old-era log with the current
	// scrubber (physical removal of the deleted subject's upsert).
	postScrub(t, base)

	// The scrub is proven by a FRESH syncable's replay of the rewritten log:
	// the erased subject's upsert is gone while every survivor still decodes
	// and arrives. Poll by re-POSTing under new ids — the scrub completes in
	// the background, so an early replay may still see the upsert.
	deadline := time.Now().Add(60 * time.Second)
	for attempt := 0; ; attempt++ {
		verify := newWebhookSink()
		id := fmt.Sprintf("era-verify-%d", attempt)
		postWebhookSyncable(t, base, id, "movie", verify.srv.URL)
		verify.awaitKeys(t, "upsert", "mv1", "mv2", "mv3", "mv4")
		if !verify.sawUpsert("subject-erased") {
			verify.srv.Close()
			break
		}
		verify.srv.Close()
		if time.Now().After(deadline) {
			t.Fatal("the scrub never removed the erased subject's upsert from the old-era log")
		}
		time.Sleep(2 * time.Second)
	}

	// 4. Restart over the rewritten dir: the post-scrub log reopens cleanly.
	node.stopGraceful(t, base)
	second := startNode(t, bin, dataDir, port)
	t.Cleanup(func() { second.stopGraceful(t, base) })
	waitReady(t, base)
}

// --- webhook sink ---

type webhookSink struct {
	srv *httptest.Server

	mu   sync.Mutex
	seen map[string]map[string]bool // op -> key -> true
}

func newWebhookSink() *webhookSink {
	s := &webhookSink{seen: map[string]map[string]bool{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Entities []struct {
				Op  string `json:"op"`
				Key string `json:"key"`
			} `json:"entities"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		for _, e := range body.Entities {
			if s.seen[e.Op] == nil {
				s.seen[e.Op] = map[string]bool{}
			}
			// Entity keys are bytes; the webhook payload base64-encodes them.
			key := e.Key
			if decoded, derr := base64.StdEncoding.DecodeString(e.Key); derr == nil {
				key = string(decoded)
			}
			s.seen[e.Op][key] = true
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	return s
}

func (s *webhookSink) sawUpsert(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen["upsert"][key]
}

func (s *webhookSink) awaitKeys(t *testing.T, op string, keys ...string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		missing := []string{}
		for _, k := range keys {
			if !s.seen[op][k] {
				missing = append(missing, k)
			}
		}
		s.mu.Unlock()
		if len(missing) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("webhook sink never received %s of %v (replay of the old log did not complete)", op, keys)
}

// --- helpers ---

func postWebhookSyncable(t *testing.T, base, id, topic, sinkURL string) {
	t.Helper()
	body := fmt.Sprintf("[syncable]\nname = %q\ntype = \"http\"\n\n[http]\ntopic = %q\nurl = %q\n", id, topic, sinkURL)
	req, err := http.NewRequest(http.MethodPost, base+"/v1/syncable/"+id, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/toml")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST syncable %s: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("POST syncable %s: status %d", id, resp.StatusCode)
	}
}

func postScrub(t *testing.T, base string) {
	t.Helper()
	resp, err := http.Post(base+"/v1/scrub", "application/json", nil) //nolint:gosec // G107: fixed loopback URL
	if err != nil {
		t.Fatalf("POST /v1/scrub: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("POST /v1/scrub: status %d", resp.StatusCode)
	}
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		bs, err := os.ReadFile(path) //nolint:gosec // test fixture under testdata
		if err != nil {
			return err
		}
		return os.WriteFile(target, bs, 0o644)
	})
	if err != nil {
		t.Fatalf("copy fixture: %v", err)
	}
}
