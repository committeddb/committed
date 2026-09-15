//go:build upgrade

// capture_fixture_test.go provisions the old-binary data-dir fixtures
// old_datadir_test.go replays. Fixture data dirs are gitignored, never
// committed: when one is missing, captureFixture builds the era's RELEASED
// binary from its git tag (a temp worktree), boots it, seeds it through its
// own HTTP API, and caches the resulting data dir under testdata/. The bytes
// are therefore always genuinely old-binary-written — the pinned contract is
// the tag plus this seed, not a checked-in blob.
//
// The standard seed: type "movie"; upserts mv1..mv3; an upsert + RTBF delete
// for "subject-erased"; a post-delete upsert mv4. A CDC era additionally
// ingests from a throwaway MySQL container (type "cdcrow": a 3-row snapshot,
// the snapshot-closing refresh-boundary MARKER, one streamed row) — the
// ingest-written byte surface (markers, generations, provenance stamps, and
// old-regime ingest-dedup records in bbolt) that direct proposals can never
// produce. The capture REFUSES to complete until its webhook guard has
// actually observed the marker arrive, so a fixture can never silently ship
// without the bytes it exists to pin. Docker and the release tags are
// capture-time dependencies only; replaying a cached fixture needs neither.
package upgrade_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	networktypes "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
)

// captureFixture captures era's data dir into testdata/<era>/datadir.
func captureFixture(t *testing.T, era fixtureEra) {
	t.Helper()
	t.Logf("capturing fixture %s (old-binary build + seed; first run only)", era.name)

	oldBin := buildTagBinary(t, era.name)
	nodeData := filepath.Join(t.TempDir(), "nodedata")
	require.NoError(t, os.MkdirAll(nodeData, 0o755))

	// The CDC source, when this era carries one, starts before the node so
	// the snapshot finds its rows.
	var sourceDSN, sourceURL string
	if era.cdc {
		sourceDSN, sourceURL = startCaptureMySQL(t)
	}

	port := freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	node := startNode(t, oldBin, nodeData, port, "FIXTURE_DB_PASSWORD=secret")
	stopped := false
	stopNode := func() {
		if !stopped {
			stopped = true
			node.stopGraceful(t, base)
		}
	}
	defer stopNode()
	waitReady(t, base)

	// --- the standard direct-proposal corpus ---
	postType(t, base, "movie")
	requireTypeListed(t, base, "movie")
	for _, body := range []string{
		`{"entities":[{"typeId":"movie","key":"mv1","data":{"title":"one"}}]}`,
		`{"entities":[{"typeId":"movie","key":"mv2","data":{"title":"two"}}]}`,
		`{"entities":[{"typeId":"movie","key":"mv3","data":{"title":"three"}}]}`,
		`{"entities":[{"typeId":"movie","key":"subject-erased","data":{"pii":"subject-erased"}}]}`,
		`{"entities":[{"typeId":"movie","key":"subject-erased","delete":true}]}`,
		`{"entities":[{"typeId":"movie","key":"mv4","data":{"title":"after-delete"}}]}`,
	} {
		postBody(t, base+"/v1/proposal", "application/json", body)
	}

	// --- the CDC corpus (marker + generations + ingest stamps) ---
	if era.cdc {
		captureCDCCorpus(t, base, sourceDSN, sourceURL)
	}

	stopNode()

	// The capture succeeded end to end — install it as the cached fixture.
	dst := filepath.Join(projectRoot(t), "e2e", "upgrade", "testdata", era.name, "datadir")
	require.NoError(t, os.RemoveAll(dst))
	copyDir(t, nodeData, dst)
	t.Logf("captured %s -> %s", era.name, dst)
}

// captureCDCCorpus registers a MySQL ingestable on the old node and holds the
// capture open until its webhook guard has observed the snapshot rows, the
// refresh-boundary marker, and one streamed row actually arrive.
func captureCDCCorpus(t *testing.T, base, sourceDSN, sourceURL string) {
	t.Helper()
	postType(t, base, "cdcrow")
	requireTypeListed(t, base, "cdcrow")

	var b strings.Builder
	fmt.Fprintf(&b, "[ingestable]\nname = \"cdcrow\"\ntype = \"sql\"\n\n")
	fmt.Fprintf(&b, "[sql]\ndialect = \"mysql\"\ntopic = \"cdcrow\"\n")
	fmt.Fprintf(&b, "connectionString = %q\n", sourceURL)
	fmt.Fprintf(&b, "primaryKey = \"id\"\ntables = [\"fixrows\"]\n\n")
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonName = \"id\"\ncolumn = \"id\"\n\n")
	fmt.Fprintf(&b, "[[sql.mappings]]\njsonName = \"name\"\ncolumn = \"name\"\n")
	postBody(t, base+"/v1/ingestable/cdcrow", "text/toml", b.String())

	// The guard: a webhook syncable into an in-process sink. The marker is
	// the point of the CDC seed — a capture that never sees it must fail.
	guard := newWebhookSink()
	defer guard.srv.Close()
	postWebhookSyncable(t, base, "capture-guard", "cdcrow", guard.srv.URL)
	guard.awaitKeys(t, "upsert", "r1", "r2", "r3")
	guard.awaitKeys(t, "refresh", "")

	// One streamed (post-snapshot) row, observed end to end.
	db, err := gosql.Open("mysql", sourceDSN)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("INSERT INTO fixrows VALUES ('r4','four')")
	require.NoError(t, err)
	guard.awaitKeys(t, "upsert", "r4")

	// The guard syncable was capture scaffolding: delete it so the fixture
	// doesn't boot with a syncable pointed at a dead sink. (Its config
	// history and checkpoint bookkeeping remain in the log — more real old
	// bytes.) The delete proposal is applied when the request returns.
	req, err := http.NewRequest(http.MethodDelete, base+"/v1/syncable/capture-guard", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Less(t, resp.StatusCode, 300, "delete capture-guard syncable")
}

// startCaptureMySQL runs the throwaway CDC source (the same image and binlog
// flags the repo's MySQL docker suites use) seeded with the pre-snapshot
// rows. Returns the Go-driver DSN (for the streamed insert) and the
// mysql:// URL the ingestable config embeds — with the password as a ${VAR}
// reference, since committed rejects inline connection-string passwords.
func startCaptureMySQL(t *testing.T) (dsn, url string) {
	t.Helper()
	ctx := context.Background()
	c, err := tcmysql.Run(ctx, "mysql:9",
		tcmysql.WithDatabase("fixdb"),
		tcmysql.WithUsername("root"),
		tcmysql.WithPassword("secret"),
		testcontainers.WithCmdArgs("--gtid-mode=ON", "--enforce-gtid-consistency=ON",
			"--binlog-row-metadata=FULL"),
		testcontainers.WithWaitStrategy(
			wait.ForSQL("3306/tcp", "mysql", func(host string, port networktypes.Port) string {
				return fmt.Sprintf("root:secret@tcp(%s:%s)/fixdb", host, port.Port())
			}).WithStartupTimeout(3*time.Minute),
		),
	)
	require.NoError(t, err, "start mysql container (docker is a capture-time dependency)")
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err)
	dsn = fmt.Sprintf("root:secret@tcp(%s:%s)/fixdb", host, port.Port())
	url = fmt.Sprintf("mysql://root:${FIXTURE_DB_PASSWORD}@%s:%s/fixdb", host, port.Port())

	db, err := gosql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE fixrows (id VARCHAR(32) NOT NULL, name VARCHAR(255), PRIMARY KEY (id))")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO fixrows VALUES ('r1','one'),('r2','two'),('r3','three')")
	require.NoError(t, err)
	return dsn, url
}

// buildTagBinary builds the committed binary at a released tag in a temp git
// worktree. Needs the tag locally — `git fetch --tags` on a shallow clone.
func buildTagBinary(t *testing.T, tag string) string {
	t.Helper()
	root := projectRoot(t)
	work := filepath.Join(t.TempDir(), "src")

	add := exec.Command("git", "worktree", "add", "--detach", work, tag)
	add.Dir = root
	if out, err := add.CombinedOutput(); err != nil {
		t.Fatalf("git worktree add %s: %v\n%s(release tags are a capture-time dependency — `git fetch --tags` on a shallow clone)", tag, err, out)
	}
	t.Cleanup(func() {
		rm := exec.Command("git", "worktree", "remove", "--force", work)
		rm.Dir = root
		_ = rm.Run()
	})

	bin := filepath.Join(t.TempDir(), "committed-"+tag)
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = work
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build committed at %s: %v\n%s", tag, err, out)
	}
	return bin
}

// postBody POSTs a request body and requires a 2xx.
func postBody(t *testing.T, url, contentType, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.GreaterOrEqual(t, resp.StatusCode, 200, "POST %s", url)
	require.Less(t, resp.StatusCode, 300, "POST %s", url)
}
