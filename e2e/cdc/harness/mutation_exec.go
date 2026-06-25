//go:build docker

package harness

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// RunScript executes a mutation script against the source database. It replaces
// the old `s.Run(ctx, h.Conn())` pattern so tests drive mutations through the
// harness rather than the driver directly — the first step toward an
// engine-agnostic harness (a MySQL backing will supply a database/sql Execer in
// place of this pgx one).
func (h *Harness) RunScript(ctx context.Context, s *mutation.Script) error {
	return s.Run(ctx, pgxExecer{conn: h.pgConn})
}

// pgxExecer adapts a *pgx.Conn to mutation.Execer.
type pgxExecer struct{ conn *pgx.Conn }

func (e pgxExecer) Txn(ctx context.Context, fn func(q mutation.Querier) error) error {
	tx, err := e.conn.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(pgxQuerier{tx: tx}); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// pgxQuerier adapts a pgx.Tx to mutation.Querier.
type pgxQuerier struct{ tx pgx.Tx }

func (q pgxQuerier) Exec(ctx context.Context, query string, args ...any) error {
	_, err := q.tx.Exec(ctx, query, args...)
	return err
}

func (pgxQuerier) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }
