package sql

import (
	"fmt"
	"sort"
	"strings"
)

// nulByteClassifier is the optional dialect capability behind the NUL field
// hint. PostgreSQL TEXT columns reject an embedded U+0000 (SQLSTATE 22021)
// while MySQL and SQL Server store it — so a row that flowed through every
// other engine dead-letters only at a PG sink, with an error that names the
// byte but not the COLUMN. Only the PG dialect implements this; the
// optional-interface fork keeps the Dialect surface unchanged for engines
// that can't produce the class.
type nulByteClassifier interface {
	IsNulByteViolation(err error) bool
}

// withNulFieldHint appends the offending FIELD NAMES to a NUL-class
// destination error, so the operator learns what to fix from the
// dead-letter itself instead of hand-hunting the byte across every string
// column (the field incident: one row in 9.4M, 14 tables searched). Names
// only, NEVER values — the message becomes a permanent, Raft-replicated
// dead-letter record, and the values are row data. A non-NUL error, a
// dialect without the classifier, or a payload with no NUL-bearing string
// passes through untouched.
func withNulFieldHint(err error, dialect Dialect, jsonData any) error {
	nc, ok := dialect.(nulByteClassifier)
	if !ok || err == nil || !nc.IsNulByteViolation(err) {
		return err
	}
	fields := nulStringFields(jsonData)
	if len(fields) == 0 {
		return err
	}
	return fmt.Errorf("%w; payload field(s) %s contain U+0000, which PostgreSQL text columns cannot store — fix the value at the source (CDC delivers the correction) or exclude the column",
		err, strings.Join(fields, ", "))
}

// nulStringFields walks a decoded JSON payload and returns the (quoted)
// names of fields whose string value contains U+0000 — sorted and
// deduplicated so the hint is stable. Only field NAMES are collected.
func nulStringFields(v any) []string {
	seen := map[string]bool{}
	var walk func(name string, v any)
	walk = func(name string, v any) {
		switch x := v.(type) {
		case string:
			if name != "" && strings.ContainsRune(x, 0) {
				seen[name] = true
			}
		case map[string]any:
			for k, e := range x {
				walk(k, e)
			}
		case []any:
			for _, e := range x {
				walk(name, e)
			}
		}
	}
	walk("", v)
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, fmt.Sprintf("%q", k))
	}
	sort.Strings(out)
	return out
}
