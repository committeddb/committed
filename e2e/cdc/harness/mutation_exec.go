//go:build docker

package harness

import (
	"context"

	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// RunScript executes a mutation script against the source database. The engine
// implements mutation.Execer (one Txn per recordedTxn), so tests drive mutations
// through the harness without touching the driver directly.
func (h *Harness) RunScript(ctx context.Context, s *mutation.Script) error {
	return s.Run(ctx, h.engine)
}
