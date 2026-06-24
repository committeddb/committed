package sql

import (
	"fmt"
	"strings"
)

// CheckKeyCoverage verifies every configured primary-key column survives into a
// source DELETE's change record — i.e. that the ingest can key a tombstone for
// it. `surviving` is the set of columns the source guarantees on delete
// (PostgreSQL replica identity / MySQL binlog row image); the dialect computes
// it and skips this call entirely when the source carries the full row.
//
// The failure this guards against is silent: a DELETE whose key columns aren't
// in the change record produces no usable tombstone, so the downstream row is
// never removed. So the error is precise and actionable — it names the missing
// columns and the fix — and the caller turns it into a loud, build-time refusal
// rather than letting the ingest run and quietly drop deletes.
func CheckKeyCoverage(primaryKey, surviving []string, table, fix string) error {
	have := make(map[string]bool, len(surviving))
	for _, c := range surviving {
		have[strings.ToLower(c)] = true
	}
	var missing []string
	for _, k := range primaryKey {
		if !have[strings.ToLower(k)] {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf(
		"ingest source table %q would silently drop deletes: a DELETE's change record does not carry primary-key column(s) %s; %s",
		table, strings.Join(missing, ", "), fix)
}
