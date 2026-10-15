//go:build docker || integration

package dialects_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/committeddb/committed/internal/cluster/syncable/sql"
)

// Window frames against their oracle: the frames ticket's three acceptance
// read models, plus RANGE offsets over a nullable number in both
// directions, each equal to PostgreSQL's own frame after every step
// (windowOracle: projection_window_docker_test.go).

// 1. A 7-reading moving average per sensor.
func TestPostgreSQLIntegration_ProjWindowFrameMovingAverage(t *testing.T) {
	table := uniqueTable(t)
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"id"},
		Columns:    []sql.ProjectionColumn{{Name: "id", SQLType: "VARCHAR(32)"}, {Name: "ma7", SQLType: "NUMERIC(18,2)"}},
		Stages: []sql.ProjectionStage{{
			Name: "readings", From: "sensor-readings", Reduce: "window",
			PartitionBy: []string{"$.sensor"}, WindowOrderBy: []string{"$.at::timestamptz"}, WindowTieBy: []string{"$.id"},
			Emit: []sql.StageEmit{{Field: "ma7", Expr: "round(avg($.reading) over (rows between 6 preceding and current row), 2)"}},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "readings", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{{Column: "ma7", From: "$.ma7"}}}}}},
	}
	o := newWindowOracle(t, cfg, "sensor-readings", `id TEXT PRIMARY KEY, sensor TEXT, at TIMESTAMPTZ, reading NUMERIC`,
		[]string{"id", "sensor", "at", "reading"}, map[string]string{"at": "::timestamptz", "reading": "::numeric"})
	got := `SELECT id, ma7::numeric(18,2)::text FROM %q ORDER BY id`
	want := `SELECT id, round(avg(reading) OVER (PARTITION BY sensor ORDER BY at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)::numeric(18,2)::text
		FROM %q ORDER BY id`

	rd := func(id, sensor, at, reading string) map[string]any {
		return map[string]any{"id": id, "sensor": sensor, "at": at, "reading": json.Number(reading)}
	}
	readings := []string{"20.5", "21", "19.75", "22", "23.5", "22.25", "21", "20", "18.5", "19"}
	for i, r := range readings {
		o.upsert(rd(string(rune('a'+i)), "s1", fmt.Sprintf("2024-05-01T00:%02d:00Z", i*5), r))
	}
	o.upsert(rd("z1", "s2", "2024-05-01T00:00:00Z", "5"))
	o.assertMatches("in-order readings", got, want)

	o.upsert(rd("late", "s1", "2024-05-01T00:17:00Z", "40"))
	o.assertMatches("a late reading shifts the next seven averages", got, want)

	o.del("c")
	o.assertMatches("a delete", got, want)
}

// 2. Login velocity: logins in the hour before each login, flagged at 3+
// into a fraud-signal table downstream.
func TestPostgreSQLIntegration_ProjWindowFrameLoginVelocity(t *testing.T) {
	table := uniqueTable(t)
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"id"},
		Columns: []sql.ProjectionColumn{
			{Name: "id", SQLType: "VARCHAR(32)"}, {Name: "user_id", SQLType: "VARCHAR(32)"}, {Name: "hour_n", SQLType: "INT"},
		},
		Stages: []sql.ProjectionStage{
			{
				Name: "velocity", From: "logins", Reduce: "window",
				PartitionBy: []string{"$.user_id"}, WindowOrderBy: []string{"$.at::timestamptz"},
				Emit: []sql.StageEmit{
					{Field: "id", From: "$.id"},
					{Field: "user_id", From: "$.user_id"},
					{Field: "hour_n", Expr: "count(*) over (range between interval '1 hour' preceding and current row)"},
				},
			},
			{
				Name: "fraud", From: "velocity", KeyPath: []string{"$.id"},
				When: []sql.WhenClause{{Expr: "$.hour_n >= 3"}},
				Emit: []sql.StageEmit{{Field: "user_id", From: "$.user_id"}, {Field: "hour_n", From: "$.hour_n"}},
			},
		},
		Sources: []sql.ProjectionSource{{FromStage: "fraud", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "user_id", From: "$.user_id"}, {Column: "hour_n", From: "$.hour_n"},
		}}}}},
	}
	o := newWindowOracle(t, cfg, "logins", `id TEXT PRIMARY KEY, user_id TEXT, at TIMESTAMPTZ`,
		[]string{"id", "user_id", "at"}, map[string]string{"at": "::timestamptz"})
	got := `SELECT id, user_id, hour_n FROM %q ORDER BY id`
	want := `SELECT id, user_id, n FROM (
		SELECT id, user_id, count(*) OVER (PARTITION BY user_id ORDER BY at RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS n FROM %q) s
		WHERE n >= 3 ORDER BY id`

	in := func(id, user, at string) map[string]any { return map[string]any{"id": id, "user_id": user, "at": at} }
	o.upsert(in("l1", "u1", "2024-06-01T10:00:00Z"))
	o.upsert(in("l2", "u1", "2024-06-01T10:20:00Z"))
	o.upsert(in("l3", "u1", "2024-06-01T10:50:00Z"))
	o.upsert(in("l4", "u1", "2024-06-01T11:10:00Z"))
	o.upsert(in("l5", "u1", "2024-06-01T13:00:00Z"))
	o.upsert(in("l6", "u2", "2024-06-01T10:00:00Z"))
	o.upsert(in("l7", "u1", "2024-06-01T11:10:00Z")) // a peer of l4: both count it
	o.assertMatches("in-order logins with a peer", got, want)

	o.upsert(in("l8", "u2", "2024-06-01T09:30:00Z"))
	o.upsert(in("l9", "u2", "2024-06-01T09:45:00Z"))
	o.assertMatches("backfilled logins push u2 over the threshold", got, want)

	o.del("l2")
	o.assertMatches("a delete drops flags", got, want)
}

// 3. last_value over the whole partition: every row carries the final
// status (the frame that makes last_value useful).
func TestPostgreSQLIntegration_ProjWindowFrameFinalStatus(t *testing.T) {
	table := uniqueTable(t)
	cfg := &sql.ProjectionConfig{
		Table:      table,
		PrimaryKey: []string{"id"},
		Columns: []sql.ProjectionColumn{
			{Name: "id", SQLType: "VARCHAR(32)"}, {Name: "final", SQLType: "VARCHAR(32)"}, {Name: "first3", SQLType: "VARCHAR(32)"},
		},
		Stages: []sql.ProjectionStage{{
			Name: "final", From: "job-status", Reduce: "window",
			PartitionBy: []string{"$.job"}, WindowOrderBy: []string{"$.at::timestamptz"}, WindowTieBy: []string{"$.seq"},
			Emit: []sql.StageEmit{
				{Field: "final", Expr: "last_value($.status) over (rows between unbounded preceding and unbounded following)"},
				{Field: "first3", Expr: "first_value($.status) over (rows between 2 following and 4 following)"},
			},
		}},
		Sources: []sql.ProjectionSource{{FromStage: "final", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
			{Column: "final", From: "$.final"}, {Column: "first3", From: "$.first3"},
		}}}}},
	}
	o := newWindowOracle(t, cfg, "job-status", `id TEXT PRIMARY KEY, job TEXT, status TEXT, at TIMESTAMPTZ, seq INT`,
		[]string{"id", "job", "status", "at", "seq"}, map[string]string{"at": "::timestamptz"})
	got := `SELECT id, final, first3 FROM %q ORDER BY id`
	want := `SELECT id,
		last_value(status) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
		first_value(status) OVER (w ROWS BETWEEN 2 FOLLOWING AND 4 FOLLOWING)
		FROM %q WINDOW w AS (PARTITION BY job ORDER BY at, seq) ORDER BY id`

	ev := func(id, job, status, at string, seq int) map[string]any {
		return map[string]any{"id": id, "job": job, "status": status, "at": at, "seq": seq}
	}
	o.upsert(ev("a", "j1", "new", "2024-01-01T00:00:00Z", 1))
	o.upsert(ev("b", "j1", "running", "2024-01-01T01:00:00Z", 2))
	o.upsert(ev("c", "j1", "paused", "2024-01-01T02:00:00Z", 3))
	o.upsert(ev("d", "j1", "running", "2024-01-01T03:00:00Z", 4))
	o.upsert(ev("e", "j2", "new", "2024-01-01T00:00:00Z", 5))
	o.assertMatches("appends", got, want)

	o.upsert(ev("f", "j1", "done", "2024-01-01T04:00:00Z", 6))
	o.assertMatches("a new final status reaches every row", got, want)

	o.del("f")
	o.assertMatches("deleting it restores the previous final status", got, want)
}

// RANGE offsets over a nullable number, ascending and descending (nulls
// first and last): null rows sort past every value, and a null current
// row's offset frame is its peers.
func TestPostgreSQLIntegration_ProjWindowFrameRangeNulls(t *testing.T) {
	for _, dir := range []struct{ ours, pg string }{
		{"$.o::numeric", "o"},
		{"$.o::numeric desc", "o DESC"},
		{"$.o::numeric nulls first", "o NULLS FIRST"},
		{"$.o::numeric desc nulls last", "o DESC NULLS LAST"},
	} {
		t.Run(dir.pg, func(t *testing.T) {
			table := uniqueTable(t)
			cfg := &sql.ProjectionConfig{
				Table:      table,
				PrimaryKey: []string{"id"},
				Columns: []sql.ProjectionColumn{
					{Name: "id", SQLType: "VARCHAR(32)"},
					{Name: "around", SQLType: "NUMERIC"},
					{Name: "before", SQLType: "NUMERIC"},
					{Name: "n_after", SQLType: "INT"},
				},
				Stages: []sql.ProjectionStage{{
					Name: "w", From: "pts", Reduce: "window",
					PartitionBy: []string{"$.grp"}, WindowOrderBy: []string{dir.ours},
					Emit: []sql.StageEmit{
						{Field: "around", Expr: "sum($.v) over (range between 2 preceding and 1 following)"},
						{Field: "before", Expr: "max($.v) over (range between unbounded preceding and 1 preceding)"},
						{Field: "n_after", Expr: "count(*) over (range between 1 following and unbounded following)"},
					},
				}},
				Sources: []sql.ProjectionSource{{FromStage: "w", Rules: []sql.ProjectionRule{{Set: []sql.ProjectionSet{
					{Column: "around", From: "$.around"}, {Column: "before", From: "$.before"}, {Column: "n_after", From: "$.n_after"},
				}}}}},
			}
			o := newWindowOracle(t, cfg, "pts", `id TEXT PRIMARY KEY, grp TEXT, o NUMERIC, v NUMERIC`,
				[]string{"id", "grp", "o", "v"}, map[string]string{"o": "::numeric", "v": "::numeric"})
			got := `SELECT id, around::text, before::text, n_after FROM %q ORDER BY id`
			want := `SELECT id,
				(sum(v) OVER (w RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING))::text,
				(max(v) OVER (w RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING))::text,
				count(*) OVER (w RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
				FROM %q WINDOW w AS (PARTITION BY grp ORDER BY ` + dir.pg + `) ORDER BY id`

			pt := func(id string, o any, v string) map[string]any {
				return map[string]any{"id": id, "grp": "g", "o": o, "v": json.Number(v)}
			}
			o.upsert(pt("a", json.Number("1"), "1"))
			o.upsert(pt("b", json.Number("2.5"), "2"))
			o.upsert(pt("c", json.Number("3"), "4"))
			o.upsert(pt("d", nil, "8"))
			o.upsert(pt("e", json.Number("6"), "16"))
			o.upsert(pt("f", nil, "32"))
			o.upsert(pt("g", json.Number("3"), "64"))
			o.assertMatches("values, peers, and nulls", got, want)

			o.upsert(pt("b", json.Number("5"), "2"))
			o.assertMatches("a value moves", got, want)

			o.del("c")
			o.assertMatches("a delete", got, want)
		})
	}
}
