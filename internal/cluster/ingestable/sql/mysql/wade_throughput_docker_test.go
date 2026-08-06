//go:build docker

package mysql_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// TestMysqlWadeThroughputMeasurement is a MEASUREMENT, not a regression
// gate: it reports the clean re-delivery wade rate — how many binlog
// entities/s committed chews through when an ingest resumes from an old
// position and the server re-streams known history — on an otherwise idle
// box. The field incident observed ~75 entities/s during a re-delivery that
// was CONFOUNDED by a concurrent 30-mirror replay; this number is the
// unconfounded baseline that decides whether 75/s was plausible contention
// degradation or evidence the incident's measurement window caught
// something else. It asserts only completeness (every row re-observed), and
// logs the rates — no throughput floor, so it can never flake on a slow CI
// box.
//
// Run it ALONE for clean numbers (-run TestMysqlWadeThroughput): the binlog
// is server-wide, so other tests' history in the same container session
// adds foreign events to the wade (cheaper per-event — they drop at the row
// filter — but they skew the divisor).
//
// COMMITTED_WADE_ROWS scales the row count (default 10000).
func TestMysqlWadeThroughputMeasurement(t *testing.T) {
	rows := 10000
	if v := os.Getenv("COMMITTED_WADE_ROWS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		rows = n
	}
	table := "wade_throughput_table"

	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk INT NOT NULL, val TEXT, PRIMARY KEY (pk));", table))
	require.NoError(t, err)

	config := &sql.Config{
		Type: &cluster.Type{ID: "wade-throughput", Name: "wade-throughput"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{table},
	}

	countEntities := func(pr <-chan *cluster.Proposal, po <-chan cluster.Position, want int, deadline time.Duration) (first, last time.Time) {
		t.Helper()
		seen := 0
		timeout := time.After(deadline)
		for seen < want {
			select {
			case p := <-pr:
				for _, e := range p.Entities {
					if e.Type != nil && e.Type.ID == "wade-throughput" && !e.IsDelete() {
						if seen == 0 {
							first = time.Now()
						}
						seen++
						last = time.Now()
					}
				}
			case <-po:
			case <-timeout:
				t.Fatalf("timed out with %d of %d entities", seen, want)
			}
		}
		return first, last
	}

	// --- Phase 1: snapshot the EMPTY table; the post-snapshot checkpoint is
	// the rewind target (it predates every row written below, so a resume
	// from it re-delivers all of them).
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 1024)
	po1 := make(chan cluster.Position, 1024)
	dialect := &mysql.MySQLDialect{}
	go func() { _ = dialect.Ingest(ctx1, config, nil, 0, pr1, po1) }()

	var rewindPos cluster.Position
	select {
	case rewindPos = <-po1:
	case <-time.After(30 * time.Second):
		t.Fatal("empty-table snapshot never checkpointed")
	}

	// --- Phase 2: write the history and let the live stream deliver it (the
	// live-CDC rate falls out as a byproduct; inserts overlap streaming, so
	// treat it as a floor, not a precise number).
	seedStart := time.Now()
	stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES (?, ?)", table))
	require.NoError(t, err)
	for i := 0; i < rows; i++ {
		_, err = stmt.Exec(i, fmt.Sprintf("value-%d", i))
		require.NoError(t, err)
	}
	stmt.Close()
	seedDur := time.Since(seedStart)

	liveDeadline := time.Duration(rows/50+120) * time.Second
	liveFirst, liveLast := countEntities(pr1, po1, rows, liveDeadline)
	cancel1()

	// --- Phase 3: the wade. Resume a fresh ingest from the pre-history
	// checkpoint on the now-idle box; the server re-streams all N
	// transactions and committed re-observes every entity. First-to-last is
	// the honest wade rate (excludes connect/dump-start latency).
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 1024)
	po2 := make(chan cluster.Position, 1024)
	dialect2 := &mysql.MySQLDialect{}
	wadeStart := time.Now()
	go func() { _ = dialect2.Ingest(ctx2, config, rewindPos, 0, pr2, po2) }()

	wadeDeadline := time.Duration(rows/50+120) * time.Second
	wadeFirst, wadeLast := countEntities(pr2, po2, rows, wadeDeadline)
	cancel2()

	liveRate := float64(rows-1) / liveLast.Sub(liveFirst).Seconds()
	wadeRate := float64(rows-1) / wadeLast.Sub(wadeFirst).Seconds()
	t.Logf("WADE MEASUREMENT: rows=%d", rows)
	t.Logf("  seed (client inserts):    %v", seedDur)
	t.Logf("  live CDC first→last:      %v  (%.0f entities/s, floor — overlaps inserts)", liveLast.Sub(liveFirst), liveRate)
	t.Logf("  wade connect→first:       %v", wadeFirst.Sub(wadeStart))
	t.Logf("  wade first→last:          %v  (%.0f entities/s CLEAN)", wadeLast.Sub(wadeFirst), wadeRate)
	t.Logf("  incident observed 75 entities/s under a concurrent 30-mirror replay; clean/75 = %.0fx", wadeRate/75)
}
