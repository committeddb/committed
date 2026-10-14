package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// sample is one measured event: committed at the source, seen in the row.
type sample struct {
	version  int
	commit   time.Time
	observed time.Time
}

// tracker measures commit-to-row latency for sampled events by polling the
// read model for the streams that have unobserved samples. Commit and
// observation are both read from this process's clock.
type tracker struct {
	read  *gosql.DB
	table string

	mu      sync.Mutex
	pending map[string][]sample // stream → samples not yet seen, in version order
	done    []sample
}

func newTracker(read *gosql.DB, table string) *tracker {
	return &tracker{read: read, table: table, pending: make(map[string][]sample)}
}

func (t *tracker) add(stream string, version int, at time.Time) {
	t.mu.Lock()
	t.pending[stream] = append(t.pending[stream], sample{version: version, commit: at})
	t.mu.Unlock()
}

func (t *tracker) pendingCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, s := range t.pending {
		n += len(s)
	}
	return n
}

// run polls every interval until ctx ends.
func (t *tracker) run(ctx context.Context, interval time.Duration) {
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			t.poll(ctx)
		}
	}
}

const pollBatch = 2000

func (t *tracker) poll(ctx context.Context) {
	t.mu.Lock()
	ids := make([]string, 0, len(t.pending))
	for id := range t.pending {
		ids = append(ids, id)
	}
	t.mu.Unlock()
	for len(ids) > 0 {
		n := min(pollBatch, len(ids))
		got, err := readVersions(ctx, t.read, t.table, ids[:n])
		ids = ids[n:]
		if err != nil {
			continue // the table appears on the projection's first write
		}
		now := time.Now()
		t.mu.Lock()
		for id, v := range got {
			ps := t.pending[id]
			i := 0
			for ; i < len(ps) && ps[i].version <= v; i++ {
				ps[i].observed = now
				t.done = append(t.done, ps[i])
			}
			if i == len(ps) {
				delete(t.pending, id)
			} else {
				t.pending[id] = ps[i:]
			}
		}
		t.mu.Unlock()
	}
}

// latencies returns the observed samples' latencies, sorted, and how many
// samples were never observed.
func (t *tracker) latencies() (all []time.Duration, byMinute map[int][]time.Duration, unobserved int, start time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	byMinute = make(map[int][]time.Duration)
	for _, s := range t.done {
		if start.IsZero() || s.commit.Before(start) {
			start = s.commit
		}
	}
	for _, s := range t.done {
		d := s.observed.Sub(s.commit)
		all = append(all, d)
		m := int(s.commit.Sub(start).Minutes())
		byMinute[m] = append(byMinute[m], d)
	}
	for _, ps := range t.pending {
		unobserved += len(ps)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	for _, v := range byMinute {
		sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	}
	return all, byMinute, unobserved, start
}

// readVersions reads the version column of the given streams' rows.
func readVersions(ctx context.Context, db *gosql.DB, table string, ids []string) (map[string]int, error) {
	q := fmt.Sprintf(`SELECT order_id::text, coalesce(version, 0) FROM read.%s WHERE order_id = ANY($1::uuid[])`, table) //nolint:gosec // G201: table is orders or orders_v2, never input
	rows, err := db.QueryContext(ctx, q, "{"+strings.Join(ids, ",")+"}")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]int, len(ids))
	for rows.Next() {
		var id string
		var v int
		if err := rows.Scan(&id, &v); err != nil {
			return nil, err
		}
		out[id] = v
	}
	return out, rows.Err()
}

// row is a read-model row as text.
type row struct{ customer, status, items, total, version string }

func (e expected) row() row {
	return row{e.customer, e.status, fmt.Sprint(e.items), e.total(), fmt.Sprint(e.version)}
}

func readRows(ctx context.Context, db *gosql.DB, table string) (map[string]row, error) {
	cols := `order_id::text, coalesce(customer, ''), coalesce(status, ''),
	  coalesce(items::text, ''), coalesce(total::text, ''), coalesce(version::text, '')`
	q := fmt.Sprintf(`SELECT %s FROM read.%s`, cols, table) //nolint:gosec // G201: table is orders or orders_v2, never input
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make(map[string]row)
	for rows.Next() {
		var id string
		var r row
		if err := rows.Scan(&id, &r.customer, &r.status, &r.items, &r.total, &r.version); err != nil {
			return nil, err
		}
		out[id] = r
	}
	return out, rows.Err()
}

// compare returns how many expected rows the table is missing or has wrong,
// how many rows it has that nothing expects, and up to five examples.
func compare(want map[string]expected, got map[string]row) (wrong, extra int, examples []string) {
	for id, e := range want {
		r, ok := got[id]
		if ok && r == e.row() {
			continue
		}
		wrong++
		if len(examples) < 5 {
			examples = append(examples, fmt.Sprintf("%s: want %+v, got %+v (present %v)", id, e.row(), r, ok))
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			extra++
		}
	}
	return wrong, extra, examples
}

// waitMatch polls until table holds exactly want, or limit passes. It
// returns when it matched (zero if it never did) and the last comparison.
func waitMatch(ctx context.Context, db *gosql.DB, table string, want map[string]expected, limit time.Duration, every time.Duration) (time.Time, int, int, []string) {
	deadline := time.Now().Add(limit)
	var wrong, extra int
	var ex []string
	for time.Now().Before(deadline) && ctx.Err() == nil {
		got, err := readRows(ctx, db, table)
		if err == nil {
			if wrong, extra, ex = compare(want, got); wrong == 0 && extra == 0 {
				return time.Now(), 0, 0, nil
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(every):
		}
	}
	return time.Time{}, wrong, extra, ex
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}
