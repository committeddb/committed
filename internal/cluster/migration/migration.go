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
	"encoding/json"
	"fmt"

	"github.com/itchyny/gojq"

	"github.com/philborlin/committed/internal/cluster"
)

// Resolver fetches type definitions by (ID, Version). The chain walker
// uses it to pull each step's Migration program from storage. Storage
// layers already implement the compatible cluster.TypeResolver
// interface, so Resolver is just an alias kept at the migration-package
// boundary for clarity and easy mocking.
type Resolver interface {
	ResolveType(ref cluster.TypeRef) (*cluster.Type, error)
}

// Chain walks the type history for typeID from stampedVersion up to
// latestVersion and applies each registered Migration program to data
// in order. Returns the transformed JSON bytes. Versions whose
// Migration is empty are skipped (no-op step). If stampedVersion equals
// or exceeds latestVersion, data is returned unchanged.
func Chain(r Resolver, typeID string, stampedVersion, latestVersion int, data []byte) ([]byte, error) {
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
		next, err := apply(t.Migration, current)
		if err != nil {
			return nil, fmt.Errorf("migration chain: type %s v%d->v%d: %w", typeID, v-1, v, err)
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

// apply runs a single jq program against a JSON document and returns
// the first produced value as JSON bytes. Programs that produce zero or
// multiple values are treated as errors — a migration must produce
// exactly one transformed document per input document.
func apply(program, data []byte) ([]byte, error) {
	q, err := gojq.Parse(string(program))
	if err != nil {
		return nil, fmt.Errorf("parse jq: %w", err)
	}

	var input any
	if err := json.Unmarshal(data, &input); err != nil {
		return nil, fmt.Errorf("unmarshal entity data: %w", err)
	}

	iter := q.Run(input)
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
