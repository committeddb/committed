package sql

import (
	"fmt"
	"strings"

	"github.com/committeddb/committed/internal/cluster"
)

// applyJSONColumnHints resolves a spec's jsonColumns onto its mappings:
// each named column (matched case-insensitively, the same tolerance every
// other column reference gets) marks its Mapping with JSONHint, so the
// shared decode (BuildEntityJSON) renders that column as real,
// canonicalized JSON instead of an escaped string. Runs AFTER map-all
// expansion, so the hint works for inferred mappings too. A name matching
// no mapped column is a loud FieldError — a typo'd hint would otherwise
// silently leave the column an escaped string, the exact failure shape
// the hint exists to fix.
func applyJSONColumnHints(spec *TopicSpec) error {
	for _, name := range spec.JSONColumns {
		matched := false
		for i := range spec.Mappings {
			if strings.EqualFold(spec.Mappings[i].SQLColumn, name) {
				spec.Mappings[i].JSONHint = true
				matched = true
			}
		}
		if !matched {
			return &cluster.FieldError{
				Field: "sql.jsonColumns",
				Issue: fmt.Sprintf("names column %q, which is not a mapped column of this topic (check the spelling, or add a mapping / mapAllColumns)", name),
			}
		}
	}
	return nil
}
