// Package eventlog defines the permanent event-storage boundary. Record IDs are
// stable Raft indexes at the application boundary; payloads are opaque here.
// Protobuf decoding, applied visibility, scrub selection, replay policy, and
// protected multi-call read lifetimes belong to the caller above this interface.
package eventlog

import (
	"context"
	"errors"
	"sync"
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

// Cursor has Seek semantics with a private reusable position. Repeating or
// moving the requested ID backwards is allowed; the caller controls progress.
// EOF (ErrNotFound) is temporary: later appends can become visible. Rewrites
// invalidate physical hints before the next read. Payloads remain caller-owned.
// Cursors are concurrency-safe, do not pin a generation, and must be closed to
// promptly release retained memory. Closing the parent invalidates its cursors.
type Cursor interface {
	Seek(uint64) (Record, error)
	Close() error
}

// Appender is the logical-record write boundary shared by EventLog backends.
// Batches retain their IDs and are validated before writing. Calls serialize;
// an I/O failure can leave a durable prefix and requires reconciliation.
type Appender interface {
	Append([]Record) error
	// LastAppended reports original append progress, including erased records.
	// ok is false only for empty append history; ID zero is valid.
	LastAppended() (id uint64, ok bool, err error)
}

// Lookup is the exact logical-record read boundary shared by EventLog backends.
// Missing IDs return ErrNotFound. Returned payloads are caller-owned.
type Lookup interface {
	Read(uint64) (Record, error)
}

// SealedCompressor is an optional background-storage capability. One call
// compresses at most one sealed segment and reports whether work was done.
// Backends that encode segments at creation need not expose this capability.
// The owner coordinates file-layout readers and backend replacement.
type SealedCompressor interface {
	CompressNextSealed() (bool, error)
}

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
// Close is idempotent. Migration between formats is outside this contract.
type EventLog interface {
	BackupSource
	Appender
	Lookup
	NewCursor() Cursor
	Seek(uint64) (Record, error)
	Scan(context.Context, Coverage, func(Record) error) error
	// ScanReverse visits at most limit survivors, newest first, in one storage
	// view. False stops successfully. The count includes callbacks that fail.
	// limit must be nonnegative; callbacks must not reenter the log. The limit
	// bounds delivery, not physical I/O within a selected append file.
	ScanReverse(context.Context, int, func(Record) (bool, error)) (int, error)
	Rewrite(context.Context, uint64, Transform) (RewriteResult, error)
	// RewriteWithPublicationLock holds a non-nil caller lock across publication.
	// Backends may acquire it earlier. Matching read lifetimes may perform reads,
	// never mutations. Lock acquisition is not context-cancelable.
	RewriteWithPublicationLock(context.Context, uint64, Transform, sync.Locker) (RewriteResult, error)
	// Generation reports the selected logical rewrite generation. Appends and
	// reclamation do not advance it. After publication failure, reopen before
	// consulting it. It identifies a selection, not scrub completion or a plan.
	Generation() (uint64, error)
	Reclaim(context.Context) (ReclaimResult, error)
	Close() error
}
