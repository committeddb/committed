//go:build multinode

package multinode_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestMultiNodePostgresIngest_LeaderCrashesLoseNoTransaction streams one-row
// Postgres transactions into a real 3-node cluster while SIGKILLing the
// leader, which runs the ingest worker, and asserts every source row reaches
// the log. A crash between handing a transaction to the worker and committing
// it must leave the transaction to the next owner: Postgres never streams
// below a slot's confirmed position, so the slot must not have been confirmed
// past it. The log is read back through a keyless mirror syncable.
func TestMultiNodePostgresIngest_LeaderCrashesLoseNoTransaction(t *testing.T) {
	buildBinary(t)
	ctx := context.Background()
	const pw = "multinodepw"
	pg, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("app"), tcpostgres.WithUsername("committed"), tcpostgres.WithPassword(pw),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmdArgs("-c", "wal_level=logical", "-c", "wal_sender_timeout=5s"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	src, err := gosql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })
	_, err = src.ExecContext(ctx, `CREATE TABLE events (id bigserial PRIMARY KEY, n int NOT NULL); CREATE SCHEMA read`)
	require.NoError(t, err)

	u, err := url.Parse(dsn)
	require.NoError(t, err)
	committedURL := fmt.Sprintf("postgres://committed:${APP_DB_PASSWORD}@%s/app?sslmode=disable", u.Host)

	nodes := startCluster(t, 3, "APP_DB_PASSWORD="+pw)
	base := nodes[0].base()
	postTOML(t, base, "/v1/type/ev", "[type]\nname = \"ev\"\n")
	postTOML(t, base, "/v1/ingestable/ev", fmt.Sprintf(`[ingestable]
name = "ev"
type = "sql"
[sql]
dialect = "postgres"
topic = "ev"
connectionString = %q
primaryKey = "id"
tables = ["events"]
mapAllColumns = true
[sql.options]
slotName = "committed_ev"
publication = "committed_ev"
`, committedURL))
	postTOML(t, base, "/v1/database/read", fmt.Sprintf("[database]\nname = \"read\"\ntype = \"sql\"\n[sql]\ndialect = \"postgres\"\nconnectionString = %q\n",
		strings.Replace(committedURL, "sslmode=disable", "search_path=read&sslmode=disable", 1)))
	postTOML(t, base, "/v1/syncable/ev_mirror", `[syncable]
name = "ev_mirror"
type = "sql"
[sql]
topic = "ev"
db = "read"
table = "ev_mirror"
[[sql.mappings]]
jsonPath = "$.id"
column = "id"
type = "BIGINT"
`)
	waitIngestStreaming(t, nodes, "ev")

	// One row per source transaction, until told to stop.
	var written atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(40 * time.Millisecond):
			}
			if _, err := src.ExecContext(ctx, `INSERT INTO events (n) VALUES (1)`); err == nil {
				written.Add(1)
			}
		}
	}()

	// Crash the leader mid-stream, repeatedly.
	for range 4 {
		time.Sleep(3 * time.Second)
		var live *clusterNode
		for _, n := range nodes {
			if !n.stopped {
				live = n
				break
			}
		}
		leader := leaderID(t, live.base())
		victim := nodes[leader-1]
		t.Logf("SIGKILL leader node %d with %d rows written", leader, written.Load())
		victim.kill(t)
		time.Sleep(2 * time.Second)
		victim.restart(t)
		waitReady(t, victim.base())
	}
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	var want int
	require.NoError(t, src.QueryRowContext(ctx, `SELECT count(*) FROM events`).Scan(&want))
	require.Positive(t, want)
	// Wait while the mirror keeps growing; a minute without progress short of
	// every row is a loss.
	got, last, lastChange := 0, -1, time.Now()
	for time.Since(lastChange) < time.Minute {
		if err := src.QueryRowContext(ctx, `SELECT count(DISTINCT id) FROM read.ev_mirror`).Scan(&got); err == nil {
			if got == want {
				break
			}
			if got != last {
				last, lastChange = got, time.Now()
			}
		}
		time.Sleep(time.Second)
	}
	if got != want {
		var missing []int64
		rows, err := src.QueryContext(ctx,
			`SELECT e.id FROM events e WHERE NOT EXISTS (SELECT 1 FROM read.ev_mirror m WHERE m.id = e.id) ORDER BY e.id LIMIT 20`)
		require.NoError(t, err)
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id))
			missing = append(missing, id)
		}
		require.NoError(t, rows.Close())
		t.Fatalf("%d of %d source rows reached the log; missing (first 20): %v", got, want, missing)
	}

	var dups int
	require.NoError(t, src.QueryRowContext(ctx, `SELECT count(*) - count(DISTINCT id) FROM read.ev_mirror`).Scan(&dups))
	t.Logf("%d source rows, all in the log; %d duplicate actuals", want, dups)
}

func postTOML(t *testing.T, base, path, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "text/toml")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	require.Truef(t, resp.StatusCode/100 == 2, "POST %s: %d %s", path, resp.StatusCode, out)
}

// waitIngestStreaming waits until the node running the ingestable's worker
// reports it streaming.
func waitIngestStreaming(t *testing.T, nodes []*clusterNode, id string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			resp, err := http.Get(n.base() + "/v1/ingestable/" + id + "/status") //nolint:gosec // G107: fixed loopback URL
			if err != nil {
				continue
			}
			var st struct {
				Phase string `json:"phase"`
			}
			derr := json.NewDecoder(resp.Body).Decode(&st)
			_ = resp.Body.Close()
			if derr == nil && st.Phase == "streaming" {
				return true
			}
		}
		return false
	}, 2*time.Minute, 200*time.Millisecond, "ingestable %s never reached streaming", id)
}
