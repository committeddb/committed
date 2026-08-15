//go:build docker || integration

package mysql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

var psType = &cluster.Type{ID: "ps_topic", Name: "ps"}

const psRows = 4000

func psSeedTable(t *testing.T, table string, rows int) {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec("DROP TABLE IF EXISTS " + table)
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE " + table + " (id INT NOT NULL PRIMARY KEY, v VARCHAR(64))")
	require.NoError(t, err)
	// Multi-row inserts keep seeding fast (the WAL-side fsync bound doesn't
	// apply here — this is the SOURCE database — but round-trips add up).
	const per = 500
	for lo := 1; lo <= rows; lo += per {
		var b strings.Builder
		b.WriteString("INSERT INTO " + table + " (id, v) VALUES ")
		for i := lo; i < lo+per && i <= rows; i++ {
			if i > lo {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "(%d,'v-%d')", i, i)
		}
		_, err = db.Exec(b.String())
		require.NoError(t, err)
	}
	// The chunk planner gates on information_schema.TABLE_ROWS, which is a
	// statistics estimate — refresh it so a freshly-seeded table doesn't
	// read as empty and fall back to the single stream.
	_, err = db.Exec("ANALYZE TABLE " + table)
	require.NoError(t, err)
}

func psConfig(table string, readers int) *sql.Config {
	return &sql.Config{
		ConnectionString: ingestURL,
		Type:             psType,
		Tables:           []string{table},
		PrimaryKey:       []string{"id"},
		Mappings:         []sql.Mapping{{JsonName: "id", SQLColumn: "id"}, {JsonName: "v", SQLColumn: "v"}},
		Options: map[string]string{
			"snapshot_readers": fmt.Sprintf("%d", readers),
			"batch_size":       "200",
		},
	}
}

// psDecodeChunks decodes a checkpoint's chunk state for table: the frozen
// plan with per-chunk cursors, or nil when the position carries none.
func psDecodeChunks(t *testing.T, pos []byte, table string) *dialectpb.TableChunkProgress {
	t.Helper()
	decoded := &dialectpb.MySQLBinLogPosition{}
	require.NoError(t, proto.Unmarshal(pos, decoded))
	if decoded.SnapshotProgress == nil {
		return nil
	}
	return decoded.SnapshotProgress.ChunksByTable[table]
}

// psDurableRows counts the rows the chunk cursors claim as durably flushed:
// for each chunk, ids in (lower, min(lastPk, upper)]. Integer keys make the
// count exact, so the resume test can assert EXACT re-delivery (deterministic
// chunk reads, no concurrent writes).
func psDurableRows(t *testing.T, plan *dialectpb.TableChunkProgress) int {
	t.Helper()
	parse := func(s string, def int) int {
		if s == "" {
			return def
		}
		var n int
		_, err := fmt.Sscanf(s, "%d", &n)
		require.NoError(t, err)
		return n
	}
	durable := 0
	for _, c := range plan.Chunks {
		lower := parse(c.Lower, 0)
		upper := parse(c.Upper, psRows)
		cursor := parse(c.LastPk, lower)
		if c.Done || cursor > upper {
			cursor = upper
		}
		if cursor > lower {
			durable += cursor - lower
		}
	}
	return durable
}

// TestMysqlParallelSnapshotResumeParity is the kill/restart proof for the
// chunked parallel snapshot: run 1 is canceled mid-snapshot; run 2 resumes
// from the last durable checkpoint with the FROZEN chunk plan and must
// deliver exactly the rows the cursors had not yet covered — no row lost
// (union completeness + payload parity) and none of the durable prefix
// re-read (exact re-delivery accounting).
func TestMysqlParallelSnapshotResumeParity(t *testing.T) {
	const table = "ps_resume"
	psSeedTable(t, table, psRows)

	// Run 1: cancel once a good chunk of the table has flushed with at least
	// one durable checkpoint behind it.
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 256)
	po1 := make(chan cluster.Position, 64)
	done1 := make(chan error, 1)
	go func() { done1 <- (&mysql.MySQLDialect{}).Ingest(ctx1, psConfig(table, 4), nil, 0, pr1, po1) }()

	run1Keys := map[string]string{}
	var lastPos []byte
	deadline := time.After(60 * time.Second)
	for len(run1Keys) < psRows/2 || lastPos == nil {
		select {
		case p := <-pr1:
			if isRefreshMarkerProposal(p) {
				continue
			}
			for _, e := range p.Entities {
				require.Equal(t, uint64(1), e.Generation, "initial snapshot stamps epoch 1")
				run1Keys[string(e.Key)] = string(e.Data)
			}
			if p.Position != nil {
				lastPos = append([]byte(nil), p.Position...)
			}
		case pos := <-po1:
			lastPos = append([]byte(nil), pos...)
		case <-deadline:
			t.Fatalf("run 1 never reached the cancel point: %d keys", len(run1Keys))
		}
	}
	cancel1()
	<-done1

	// The captured checkpoint must carry a real mid-snapshot chunk plan —
	// otherwise this test silently degrades to the single-stream path.
	plan := psDecodeChunks(t, lastPos, table)
	require.NotNil(t, plan, "the snapshot must have taken the chunked path")
	require.GreaterOrEqual(t, len(plan.Chunks), 2, "a real plan")
	durable := psDurableRows(t, plan)
	require.Positive(t, durable, "cancel point must be past the first checkpoint")
	require.Less(t, durable, psRows, "cancel point must be mid-snapshot")
	t.Logf("run 1: %d keys delivered, %d durable at cancel, %d chunks", len(run1Keys), durable, len(plan.Chunks))

	// Run 2: resume from the durable checkpoint, drain to completion.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 256)
	po2 := make(chan cluster.Position, 64)
	done2 := make(chan error, 1)
	go func() { done2 <- (&mysql.MySQLDialect{}).Ingest(ctx2, psConfig(table, 4), lastPos, 0, pr2, po2) }()

	run2Keys := map[string]string{}
	snapshotDone := false
	deadline = time.After(120 * time.Second)
	for !snapshotDone {
		select {
		case p := <-pr2:
			if isRefreshMarkerProposal(p) {
				// The refresh-boundary marker rides the end of the initial
				// snapshot — completion.
				snapshotDone = true
				continue
			}
			for _, e := range p.Entities {
				run2Keys[string(e.Key)] = string(e.Data)
			}
		case pos := <-po2:
			decoded := &dialectpb.MySQLBinLogPosition{}
			require.NoError(t, proto.Unmarshal(pos, decoded))
			if decoded.SnapshotProgress == nil {
				snapshotDone = true // streaming-phase checkpoint: snapshot over
			}
		case <-deadline:
			t.Fatalf("run 2 never completed: %d keys", len(run2Keys))
		}
	}
	cancel2()
	<-done2

	// No row lost: the union covers every id, and payloads agree wherever
	// both runs delivered a row.
	for i := 1; i <= psRows; i++ {
		k := fmt.Sprintf("%d", i)
		_, in1 := run1Keys[k]
		v2, in2 := run2Keys[k]
		require.True(t, in1 || in2, "row %d lost across the resume", i)
		if in2 {
			require.Contains(t, v2, fmt.Sprintf(`"v-%d"`, i), "row %d payload parity", i)
		}
	}
	// No durable row re-read: chunk reads are deterministic over integer keys
	// with no concurrent writes, so run 2 delivers EXACTLY the non-durable
	// remainder.
	require.Equal(t, psRows-durable, len(run2Keys),
		"run 2 must deliver exactly the rows the durable cursors had not covered")
	t.Logf("run 2: %d keys (exactly the %d non-durable rows)", len(run2Keys), psRows-durable)
}

// TestMysqlParallelSnapshotSpeedup measures the wall-clock effect of 4
// readers against the single stream on the same seeded table, in this
// harness (localhost source, test-drained channels — the least favorable
// setting for read parallelism, since there is no network round-trip to
// overlap; a remote replica gains more). Measured 2.8–2.9x across runs on
// dev hardware; the ≥2x floor is the ticket's scaling criterion, not exact
// linearity.
func TestMysqlParallelSnapshotSpeedup(t *testing.T) {
	const table = "ps_speed"
	const rows = 100000
	psSeedTable(t, table, rows)

	run := func(readers int) time.Duration {
		cfg := psConfig(table, readers)
		cfg.Options["batch_size"] = "1000"
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pr := make(chan *cluster.Proposal, 1024)
		po := make(chan cluster.Position, 256)
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- (&mysql.MySQLDialect{}).Ingest(ctx, cfg, nil, 0, pr, po) }()
		seen := 0
		deadline := time.After(180 * time.Second)
		for seen < rows {
			select {
			case p := <-pr:
				if isRefreshMarkerProposal(p) {
					continue
				}
				seen += len(p.Entities)
			case <-po:
			case <-deadline:
				t.Fatalf("snapshot with %d readers never delivered all rows: %d", readers, seen)
			}
		}
		elapsed := time.Since(start)
		cancel()
		<-done
		return elapsed
	}

	single := run(1)
	parallel := run(4)
	ratio := float64(single) / float64(parallel)
	t.Logf("snapshot wall clock: single=%v parallel(4)=%v speedup=%.2fx", single, parallel, ratio)
	require.GreaterOrEqual(t, ratio, 2.0, "4 readers must at least halve the snapshot wall clock")
}
