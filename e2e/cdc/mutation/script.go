//go:build docker

// Package mutation defines the scripted-mutation DSL used by CDC
// scenario tests. A Script records a sequence of Postgres operations
// AND the expected proposal stream that committed should produce in
// response. Each op carries both the SQL to execute and the Entity it
// expects to see — no hidden state, no post-hoc computation.
package mutation

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/philborlin/committed/e2e/cdc/dataset"
)

// Script is a sequence of transactions to execute, plus their expected
// proposals. Build with NewScript then call Txn / Rollback / Exec /
// Insert / Update / Delete in order.
type Script struct {
	txns []*recordedTxn
}

// recordedTxn is one BEGIN…COMMIT (or BEGIN…ROLLBACK) block. The ops
// inside a committed txn become a single Proposal in committed's log
// (per postgres.go:332 — one flush per CommitMessage). A rollback txn
// produces zero proposals.
type recordedTxn struct {
	ops      []*recordedOp
	rollback bool
	autoOnly bool // implicit one-op txn from a top-level call
}

type opKind int

const (
	opInsert opKind = iota
	opUpdate
	opDelete
)

// recordedOp is one row-level mutation. expected is the Entity that
// should appear in committed's proposal as a result. For deletes the
// expected.Data depends on the table's REPLICA IDENTITY: FULL gives a
// full old row, DEFAULT gives PK-only.
type recordedOp struct {
	kind     opKind
	table    string
	pkCol    string
	pkVal    any
	row      map[string]any // full row state for the post-image (insert/update)
	expected *Expected
}

// Expected describes one entity the oracle expects to see in committed's
// proposal stream. Topic is the table name (ingestable topic = table
// name in our harness setup). Key is the stringified primary key. Data
// is the JSON shape committed should produce (column → value).
type Expected struct {
	Topic string
	Key   string
	Data  map[string]any
}

// NewScript returns an empty Script.
func NewScript() *Script { return &Script{} }

// Txn appends a committing transaction. The callback adds ops via the
// Txn methods; on return the txn closes (COMMIT). The resulting ops
// land in one Proposal in commit order.
func (s *Script) Txn(fn func(t *Txn)) {
	rec := &recordedTxn{}
	t := &Txn{rec: rec}
	fn(t)
	s.txns = append(s.txns, rec)
}

// Rollback appends a transaction that runs the same ops but rolls back
// instead of committing. The oracle expects zero proposals from this
// block. Use to verify pgoutput correctly drops uncommitted changes.
func (s *Script) Rollback(fn func(t *Txn)) {
	rec := &recordedTxn{rollback: true}
	t := &Txn{rec: rec}
	fn(t)
	s.txns = append(s.txns, rec)
}

// Insert appends an implicit single-op transaction. Equivalent to
// s.Txn(func(t){ t.Insert(...) }) but easier to read for one-shots.
func (s *Script) Insert(table string, row map[string]any) {
	s.Txn(func(t *Txn) { t.Insert(table, row) })
	s.txns[len(s.txns)-1].autoOnly = true
}

// Update is the one-shot form for an UPDATE.
func (s *Script) Update(table string, row map[string]any) {
	s.Txn(func(t *Txn) { t.Update(table, row) })
	s.txns[len(s.txns)-1].autoOnly = true
}

// Delete is the one-shot form for a DELETE.
func (s *Script) Delete(table string, row map[string]any) {
	s.Txn(func(t *Txn) { t.Delete(table, row) })
	s.txns[len(s.txns)-1].autoOnly = true
}

// Txn is the per-transaction op recorder passed to Script.Txn /
// Script.Rollback callbacks.
type Txn struct {
	rec *recordedTxn
}

// Insert records an INSERT of the given full row. row must contain all
// NOT NULL columns; the harness does not provide defaults. The expected
// Entity has Data equal to row (JSON-encoded via the standard library).
func (t *Txn) Insert(table string, row map[string]any) {
	pkCol := dataset.PrimaryKey(table)
	if pkCol == "" {
		panic(fmt.Sprintf("mutation: unknown table %q", table))
	}
	pkVal, ok := row[pkCol]
	if !ok {
		panic(fmt.Sprintf("mutation: row for %s missing primary key %q", table, pkCol))
	}
	t.rec.ops = append(t.rec.ops, &recordedOp{
		kind: opInsert, table: table, pkCol: pkCol, pkVal: pkVal, row: row,
		expected: expectedFromRow(table, pkVal, row),
	})
}

// Update records an UPDATE of the row identified by row[pkCol] to the
// values in row. row must be the FULL post-image (every column), not a
// SET list — pgoutput sends the full new tuple under REPLICA IDENTITY
// FULL, and the oracle compares against that shape.
func (t *Txn) Update(table string, row map[string]any) {
	pkCol := dataset.PrimaryKey(table)
	pkVal := row[pkCol]
	t.rec.ops = append(t.rec.ops, &recordedOp{
		kind: opUpdate, table: table, pkCol: pkCol, pkVal: pkVal, row: row,
		expected: expectedFromRow(table, pkVal, row),
	})
}

// Delete records a DELETE of the row identified by row[pkCol]. row
// must be the FULL pre-delete state of the row (because under REPLICA
// IDENTITY FULL pgoutput sends the full OLD tuple, which becomes the
// expected Entity's Data). For REPLICA IDENTITY DEFAULT tables, pass
// only the PK column.
func (t *Txn) Delete(table string, row map[string]any) {
	pkCol := dataset.PrimaryKey(table)
	pkVal := row[pkCol]
	t.rec.ops = append(t.rec.ops, &recordedOp{
		kind: opDelete, table: table, pkCol: pkCol, pkVal: pkVal, row: row,
		expected: expectedFromRow(table, pkVal, row),
	})
}

// Exec records a raw SQL statement with no expected Entity. Used by
// preflight/state-setup helpers that need to touch tables outside the
// publication (no proposal expected). Mutation tests should prefer
// Insert/Update/Delete.
func (t *Txn) Exec(query string, args ...any) {
	t.rec.ops = append(t.rec.ops, &recordedOp{
		kind: opInsert, table: "", row: nil, expected: nil,
		pkVal: rawSQL{query, args},
	})
}

type rawSQL struct {
	query string
	args  []any
}

// Run executes the script against Postgres in declaration order. Each
// recordedTxn runs inside an explicit BEGIN/COMMIT (or BEGIN/ROLLBACK).
func (s *Script) Run(ctx context.Context, conn *pgx.Conn) error {
	for ti, rec := range s.txns {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("txn %d: begin: %w", ti, err)
		}
		for oi, op := range rec.ops {
			if raw, ok := op.pkVal.(rawSQL); ok && op.expected == nil {
				if _, err := tx.Exec(ctx, raw.query, raw.args...); err != nil {
					_ = tx.Rollback(ctx)
					return fmt.Errorf("txn %d op %d: raw exec: %w", ti, oi, err)
				}
				continue
			}
			query, args := buildSQL(op)
			if _, err := tx.Exec(ctx, query, args...); err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("txn %d op %d (%s on %s pk=%v): %w",
					ti, oi, opKindString(op.kind), op.table, op.pkVal, err)
			}
		}
		if rec.rollback {
			if err := tx.Rollback(ctx); err != nil {
				return fmt.Errorf("txn %d: rollback: %w", ti, err)
			}
			continue
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("txn %d: commit: %w", ti, err)
		}
	}
	return nil
}

// Expected returns the expected proposal stream per topic. Each entry
// is one expected proposal (one per committed Postgres txn). Returned
// keyed by topic so the oracle can diff against per-topic
// Harness.Capture() output.
func (s *Script) Expected() map[string][]ExpectedProposal {
	out := make(map[string][]ExpectedProposal)
	for _, rec := range s.txns {
		if rec.rollback {
			continue
		}
		perTopic := make(map[string]*ExpectedProposal)
		for _, op := range rec.ops {
			if op.expected == nil {
				continue
			}
			pp, ok := perTopic[op.expected.Topic]
			if !ok {
				pp = &ExpectedProposal{Topic: op.expected.Topic}
				perTopic[op.expected.Topic] = pp
			}
			pp.Entities = append(pp.Entities, *op.expected)
		}
		// Append per-topic proposals in deterministic order. Topic
		// strings sorted alphabetically so failure diffs are stable.
		topics := make([]string, 0, len(perTopic))
		for tp := range perTopic {
			topics = append(topics, tp)
		}
		sort.Strings(topics)
		for _, tp := range topics {
			out[tp] = append(out[tp], *perTopic[tp])
		}
	}
	return out
}

// ExpectedProposal is one proposal the oracle expects for one topic.
// Topic = table name (= ingestable type name in our harness).
type ExpectedProposal struct {
	Topic    string
	Entities []Expected
}

// ExpectedCounts returns just the proposal count per topic — what the
// harness needs to gate Capture on. Avoids a runtime dependency from
// the harness back onto this package's full ExpectedProposal shape.
func (s *Script) ExpectedCounts() map[string]int {
	exp := s.Expected()
	counts := make(map[string]int, len(exp))
	for topic, props := range exp {
		counts[topic] = len(props)
	}
	return counts
}

// expectedFromRow builds the Expected for one op. The Key is the
// stringified primary key value (pgoutput's text representation is
// fmt.Sprintf("%v", ...) for our int/string types). The Data is the
// row map — the oracle's JSON normalization handles key ordering.
func expectedFromRow(table string, pkVal any, row map[string]any) *Expected {
	return &Expected{
		Topic: table,
		Key:   fmt.Sprintf("%v", pkVal),
		Data:  row,
	}
}

// buildSQL turns one recordedOp into a parameterized SQL statement.
// Always uses positional parameters ($1, $2…) — pgx requires them and
// they sidestep any quoting issues.
func buildSQL(op *recordedOp) (string, []any) {
	switch op.kind {
	case opInsert:
		cols := dataset.Columns(op.table)
		placeholders := make([]string, len(cols))
		args := make([]any, len(cols))
		for i, c := range cols {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = op.row[c]
		}
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			op.table, strings.Join(cols, ", "), strings.Join(placeholders, ", ")), args
	case opUpdate:
		cols := dataset.Columns(op.table)
		setParts := make([]string, 0, len(cols))
		args := make([]any, 0, len(cols)+1)
		i := 1
		for _, c := range cols {
			if c == op.pkCol {
				continue
			}
			setParts = append(setParts, fmt.Sprintf("%s=$%d", c, i))
			args = append(args, op.row[c])
			i++
		}
		args = append(args, op.pkVal)
		return fmt.Sprintf("UPDATE %s SET %s WHERE %s=$%d",
			op.table, strings.Join(setParts, ", "), op.pkCol, i), args
	case opDelete:
		return fmt.Sprintf("DELETE FROM %s WHERE %s=$1", op.table, op.pkCol),
			[]any{op.pkVal}
	}
	return "", nil
}

func opKindString(k opKind) string {
	switch k {
	case opInsert:
		return "INSERT"
	case opUpdate:
		return "UPDATE"
	case opDelete:
		return "DELETE"
	}
	return "?"
}

// JSONNormalize converts an Expected's Data into the same JSON shape
// committed produces. pgoutput emits text-encoded column values, so
// integer keys come through as strings in committed's stream. This
// helper applies the same coercion to the expected side so the oracle
// compares like with like.
func JSONNormalize(d map[string]any) string {
	// Sort keys so the JSON is byte-stable.
	keys := make([]string, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := map[string]any{}
	for _, k := range keys {
		out[k] = coerceForPgout(d[k])
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// coerceForPgout matches pgoutput's text-encoding behavior. pgoutput
// sends every column as a text string, so committed's tupleToEntity
// stores everything as string in the JSON. We coerce ints, floats, and
// nils accordingly.
func coerceForPgout(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		return x
	case int:
		return fmt.Sprintf("%d", x)
	case int32:
		return fmt.Sprintf("%d", x)
	case int64:
		return fmt.Sprintf("%d", x)
	case float64:
		return fmt.Sprintf("%v", x)
	case bool:
		if x {
			return "t"
		}
		return "f"
	}
	return fmt.Sprintf("%v", v)
}
