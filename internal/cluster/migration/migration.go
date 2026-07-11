// Package migration applies the jq transforms registered on Type
// versions to entity data, upgrading old proposals to the shape a newer
// type version expects. Syncables opted into "always-current" mode run
// proposals through Chain before Sync sees them; "as-stored" syncables
// bypass this package entirely.
//
// A single transform is the jq program stored in Type.Migration — it
// takes the shape produced by version N-1 and returns the shape version
// N expects. A Chain walks the version history from the stamped version
// up to the current latest and applies each step in sequence.
package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/itchyny/gojq"

	"github.com/committeddb/committed/internal/cluster"
)

// runTimeout bounds a single jq transform. jq has no wall-clock or iteration
// cap, so an accidental infinite recursion or a huge allocation in an
// operator-supplied program would otherwise run unbounded on the goroutine that
// invoked it — wedging the sync worker (un-cancellable, defeating shutdown) or
// the propose handler. 30s is far above any legitimate single-entity transform
// and only trips on a genuinely pathological program.
const runTimeout = 30 * time.Second

// Resolver fetches type definitions by (ID, Version). The chain walker
// uses it to pull each step's Migration program from storage. Storage
// layers already implement the compatible cluster.TypeResolver
// interface, so Resolver is just an alias kept at the migration-package
// boundary for clarity and easy mocking.
type Resolver interface {
	ResolveType(ref cluster.TypeRef) (*cluster.Type, error)
}

// Error is a migration program failing at runtime on a concrete entity —
// the failure compile-time validation can't catch. It identifies the
// failing chain step so the layer that owns metrics and dead-lettering
// (db's sync worker) can attribute the failure to a type and version pair
// instead of just the syncable that happened to trip over it. Unwraps to
// the underlying jq/JSON error.
type Error struct {
	TypeID string
	// FromVersion -> ToVersion is the chain step that failed: the
	// ToVersion program is the one that errored.
	FromVersion int
	ToVersion   int
	Err         error
}

func (e *Error) Error() string {
	return fmt.Sprintf("migration chain: type %s v%d->v%d: %v", e.TypeID, e.FromVersion, e.ToVersion, e.Err)
}

// RedactedMessage is the PII-free form to persist into the permanent, replicated
// dead-letter record. The wrapped Err is a gojq runtime error that inlines the
// entity's field values (e.g. `cannot iterate over: string ("123-45-6789")`), so
// only the classifier — the type id and the failing chain step — is safe to
// replicate; the full detail stays in this node's logs. Satisfies
// cluster.RedactedError, which recordSyncDeadLetter and
// recordTypeMigrationDeadLetter honor so migration PII never reaches the log.
func (e *Error) RedactedMessage() string {
	return fmt.Sprintf("migration chain: type %s v%d->v%d: transform failed (full detail in this node's logs)", e.TypeID, e.FromVersion, e.ToVersion)
}

func (e *Error) Unwrap() error { return e.Err }

// Error satisfies cluster.RedactedError so its full text (which inlines entity
// PII) is kept node-local while only RedactedMessage is replicated.
var _ cluster.RedactedError = (*Error)(nil)

// Chain walks the type history for typeID from stampedVersion up to
// latestVersion and applies each registered Migration program to data
// in order. Returns the transformed JSON bytes. Versions whose
// Migration is empty are skipped (no-op step). If stampedVersion equals
// or exceeds latestVersion, data is returned unchanged. A program that
// fails at runtime is reported as an *Error naming the failing step.
func Chain(ctx context.Context, r Resolver, typeID string, stampedVersion, latestVersion int, data []byte) ([]byte, error) {
	if stampedVersion >= latestVersion {
		return data, nil
	}

	current := data
	for v := stampedVersion + 1; v <= latestVersion; v++ {
		t, err := r.ResolveType(cluster.TypeRefAt(typeID, v))
		if err != nil {
			return nil, fmt.Errorf("migration chain: fetch type %s@%d: %w", typeID, v, err)
		}
		if len(t.Migration) == 0 {
			continue
		}
		next, err := Run(ctx, t.Migration, current)
		if err != nil {
			return nil, &Error{TypeID: typeID, FromVersion: v - 1, ToVersion: v, Err: err}
		}
		current = next
	}
	return current, nil
}

// Compile parses a jq program and returns an error if the program is
// syntactically invalid. Called at Type-registration time so operators
// see bad programs immediately instead of at first-sync.
func Compile(program []byte) error {
	_, err := gojq.Parse(string(program))
	return err
}

// Run executes a single jq program against a JSON document and returns
// the first produced value as JSON bytes. Programs that produce zero or
// multiple values are treated as errors — a migration must produce
// exactly one transformed document per input document. Chain calls it per
// step; ParseType's pre-flight validation calls it directly to try a
// proposed program against an operator-supplied sample payload.
func Run(ctx context.Context, program, data []byte) ([]byte, error) {
	q, err := gojq.Parse(string(program))
	if err != nil {
		return nil, fmt.Errorf("parse jq: %w", err)
	}

	// Decode with UseNumber so numeric leaves stay json.Number instead of
	// float64 — otherwise any integer above 2^53 (a BIGINT id, money value, or
	// key) is silently rounded on the round-trip and that corruption is
	// persisted to the sink. gojq handles json.Number natively (preserving large
	// integers as *big.Int), so the transform round-trips exact digits.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var input any
	if err := dec.Decode(&input); err != nil {
		return nil, fmt.Errorf("unmarshal entity data: %w", err)
	}

	// Bound execution and honor cancellation: RunWithContext observes both the
	// parent ctx (worker shutdown / request cancel) and this per-run deadline, so
	// a pathological program can't hang the goroutine that invoked it.
	ctx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()
	iter := q.RunWithContext(ctx, input)
	first, ok := iter.Next()
	if !ok {
		return nil, fmt.Errorf("jq program produced no output")
	}
	if err, isErr := first.(error); isErr {
		return nil, fmt.Errorf("jq runtime: %w", err)
	}
	if _, more := iter.Next(); more {
		return nil, fmt.Errorf("jq program produced multiple outputs (expected exactly one)")
	}

	out, err := json.Marshal(first)
	if err != nil {
		return nil, fmt.Errorf("marshal jq result: %w", err)
	}
	return out, nil
}
