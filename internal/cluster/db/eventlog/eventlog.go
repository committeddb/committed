// Package eventlog defines the permanent event-storage boundary. Record IDs are
// stable Raft indexes at the application boundary; payloads are opaque here.
// Protobuf decoding, applied visibility, scrub selection, replay policy, and
// protected multi-call read lifetimes belong to the caller above this interface.
package eventlog

import (
	"context"
	"errors"
)

var (
	ErrNotFound    = errors.New("eventlog: record not found")
	ErrInvalid     = errors.New("eventlog: invalid argument")
	ErrCorrupt     = errors.New("eventlog: corrupt storage")
	ErrClosed      = errors.New("eventlog: closed")
	ErrPoisoned    = errors.New("eventlog: recovery required")
	ErrLocked      = errors.New("eventlog: directory already owned")
	ErrUnsupported = errors.New("eventlog: unsupported format or platform")
)

type (
	Record struct {
		ID      uint64
		Payload []byte
	}
	Coverage  struct{ Start, End uint64 }
	Transform func(Record) (payload []byte, keep bool, err error)
)

// RewriteResult counts observed semantic changes, including on failure. Published
// means publication durability was confirmed; false with an error does not prove
// the old generation remains selected. Reopen to resolve uncertainty.
type (
	RewriteResult struct {
		Published      bool
		ChangedRecords uint64
	}
	ReclaimResult struct{ RemovedFiles, RemovedBytes, SkippedEntries uint64 }
)

// EventLog owns one permanent event-log directory. Methods are concurrency-safe.
// The caller must not mutate its files or use another writer outside this owner.
// Backends use different physical layouts; callers cannot infer coverage from
// filenames or assume equal generations imply equal bytes.
//
// Append validates the entire strictly increasing batch before writing, syncs
// before success, and rejects duplicate IDs. Failure can leave a durable prefix;
// LastAppended reports recovered original progress even after erasure. It returns
// ok=false only for empty append history (ID zero itself is valid).
//
// Read is exact; Seek finds the next survivor at/after an ID. Scan visits the
// half-open interval in one consistent view. Empty intervals succeed. Returned
// payloads are caller-owned; retention can retain decoded blocks. Callbacks must
// not reenter the log. Callback errors/cancellation can follow a delivered prefix.
//
// Rewrite requires a strictly newer generation, retains IDs, transforms each
// examined record once, and atomically publishes the whole result. No-op rewrites
// may publish metadata. Physical reuse is backend-specific. After preparation or
// publication failure, close/reopen before retry. Reclaim explicitly removes old
// managed payloads; publication alone does not establish physical erasure.
// Close is idempotent. This contract does not define backup capture or migration.
type EventLog interface {
	Append([]Record) error
	LastAppended() (uint64, bool, error)
	Read(uint64) (Record, error)
	Seek(uint64) (Record, error)
	Scan(context.Context, Coverage, func(Record) error) error
	Rewrite(context.Context, uint64, Transform) (RewriteResult, error)
	Reclaim(context.Context) (ReclaimResult, error)
	Close() error
}
