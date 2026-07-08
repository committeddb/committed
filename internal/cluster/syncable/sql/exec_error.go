package sql

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

// execError wraps a driver error from a per-row apply exec. A driver error can
// echo bound values — a Postgres foreign-key violation includes "Key (col)=(value)",
// and the bound parameters are the entity's key or data — so it separates the
// full detail (Error(), for node-local logs and errors.As) from a PII-free
// classifier (RedactedMessage(), persisted into the permanent, replicated
// dead-letter log). It satisfies cluster.RedactedError, which recordSyncDeadLetter
// honors: the classifier is replicated, the full detail stays on the node.
type execError struct {
	// label is committed's own classifier plus the statement's placeholder SQL,
	// which carries no bound values — e.g. "[sql.apply] exec [DELETE FROM t WHERE k = $1]".
	label string
	err   error // the driver error; may carry PII echoed from bound values
}

func (e *execError) Error() string { return fmt.Sprintf("%s: %v", e.label, e.err) }
func (e *execError) RedactedMessage() string {
	return e.label + ": driver error (full detail in this node's logs)"
}
func (e *execError) Unwrap() error { return e.err }

// execFailure builds the error for a failed per-row apply exec: an execError
// labeled with the statement's placeholder SQL, marked Permanent when the dialect
// classifies the driver error as non-retryable. cluster.Permanent double-wraps,
// so errors.As still reaches the execError (and its RedactedMessage) through it.
func execFailure(label string, err error, permanent bool) error {
	e := &execError{label: label, err: err}
	if permanent {
		return cluster.Permanent(e)
	}
	return e
}
