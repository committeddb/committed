//go:build docker

// Package oracle compares the expected proposal stream (from a
// mutation.Script) against the actual stream captured from committed.
// The comparison enforces:
//   - Proposal count and ordering across the topic — strict (commit
//     order is deterministic in pgoutput).
//   - Entity set within a proposal — multiset equality (cross-key order
//     within a single Postgres txn is plan-dependent for multi-row
//     statements, so the oracle does not require a fixed order).
//   - Per-key ordering across the topic — strict (for any one primary
//     key the sequence of entities must match exactly).
//
// On failure, Assert calls t.Errorf with a readable diff that names
// the offending topic, the proposal index, and the specific constraint
// that broke.
package oracle

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/philborlin/committed/e2e/cdc/harness"
	"github.com/philborlin/committed/e2e/cdc/mutation"
)

// Assert compares expected vs actual proposal streams per topic and
// fails the test with a readable diff on any mismatch.
//
// expected is map[topic][]ExpectedProposal — one entry per committed
// txn that produced entities on that topic.
//
// actual is map[topic][]CapturedProposal — what the harness pulled
// from committed's HTTP API after waiting for LSN catch-up.
func Assert(t *testing.T, expected map[string][]mutation.ExpectedProposal, actual map[string][]harness.CapturedProposal) {
	t.Helper()

	topics := unionTopics(expected, actual)
	for _, topic := range topics {
		exp := expected[topic]
		act := actual[topic]
		assertTopic(t, topic, exp, act)
	}
}

func assertTopic(t *testing.T, topic string, exp []mutation.ExpectedProposal, act []harness.CapturedProposal) {
	t.Helper()

	if len(exp) != len(act) {
		t.Errorf("topic %q: proposal count mismatch — expected %d, got %d\n%s",
			topic, len(exp), len(act), renderTopicDump(topic, exp, act))
		return
	}

	for i := range exp {
		if msg := diffProposal(exp[i], act[i]); msg != "" {
			t.Errorf("topic %q proposal %d: %s\n  expected: %s\n  actual:   %s",
				topic, i, msg, renderExpected(exp[i]), renderActual(act[i]))
		}
	}

	// Per-key cross-proposal ordering check. For each PK, the sequence
	// of entities expected across all proposals on this topic must be
	// a subsequence of the actual order. Because we already verified
	// per-proposal multiset equality above, this reduces to walking
	// expected and actual in proposal order and asserting that within
	// each proposal the per-key subsequence matches.
	if msg := perKeyOrderCheck(exp, act); msg != "" {
		t.Errorf("topic %q: %s", topic, msg)
	}
}

// diffProposal returns "" when the entity multiset matches, else a
// short reason string.
func diffProposal(exp mutation.ExpectedProposal, act harness.CapturedProposal) string {
	if len(exp.Entities) != len(act.Entities) {
		return fmt.Sprintf("entity count mismatch — expected %d, got %d",
			len(exp.Entities), len(act.Entities))
	}

	// Multiset comparison: group expected entities by (key, normalized
	// data JSON), do the same for actual, and assert per-group counts
	// match. This permits any cross-key ordering within the proposal.
	expCounts := tally(exp.Entities, expectedKey)
	actCounts := tally(act.Entities, capturedKey)

	if len(expCounts) != len(actCounts) {
		return fmt.Sprintf("distinct entity count mismatch (%d vs %d)",
			len(expCounts), len(actCounts))
	}
	for k, n := range expCounts {
		if actCounts[k] != n {
			return fmt.Sprintf("entity %q: expected %d occurrence(s), got %d",
				k, n, actCounts[k])
		}
	}
	for k := range actCounts {
		if _, ok := expCounts[k]; !ok {
			return fmt.Sprintf("unexpected entity %q in actual proposal", k)
		}
	}
	return ""
}

// perKeyOrderCheck ensures that for any PK, the sequence of entities
// across the topic's proposals matches in order. This catches reorderings
// where the per-proposal multiset is correct but the per-key history is
// scrambled across proposals.
func perKeyOrderCheck(exp []mutation.ExpectedProposal, act []harness.CapturedProposal) string {
	expByKey := flattenByKey(exp, len(exp))
	actByKey := flattenByKey2(act, len(act))

	for key, expSeq := range expByKey {
		actSeq := actByKey[key]
		if len(actSeq) != len(expSeq) {
			return fmt.Sprintf("per-key history mismatch for key %q: expected %d events, got %d",
				key, len(expSeq), len(actSeq))
		}
		for i := range expSeq {
			if expSeq[i] != actSeq[i] {
				return fmt.Sprintf("per-key order mismatch for key %q at event %d:\n  expected: %s\n  actual:   %s",
					key, i, expSeq[i], actSeq[i])
			}
		}
	}
	return ""
}

func flattenByKey(props []mutation.ExpectedProposal, _ int) map[string][]string {
	out := make(map[string][]string)
	for _, p := range props {
		for _, e := range p.Entities {
			out[e.Key] = append(out[e.Key], expectedKey(e))
		}
	}
	return out
}

func flattenByKey2(props []harness.CapturedProposal, _ int) map[string][]string {
	out := make(map[string][]string)
	for _, p := range props {
		for _, e := range p.Entities {
			out[e.Key] = append(out[e.Key], capturedKey(e))
		}
	}
	return out
}

func tally[T any](items []T, keyFn func(T) string) map[string]int {
	out := make(map[string]int, len(items))
	for _, it := range items {
		out[keyFn(it)]++
	}
	return out
}

// expectedKey returns the canonical "(key, normalized data)" string
// for an Expected entity. Includes the key so two entities with the
// same data but different PKs are still distinct.
func expectedKey(e mutation.Expected) string {
	return e.Key + "|" + mutation.JSONNormalize(e.Data)
}

// capturedKey returns the canonical "(key, normalized data)" string
// for a CapturedEntity. Normalizes the JSON so that map ordering
// doesn't cause spurious mismatches.
func capturedKey(e harness.CapturedEntity) string {
	return e.Key + "|" + normalizeJSON(e.Data)
}

// normalizeJSON re-serializes a JSON string with sorted keys. If the
// input isn't valid JSON it returns it unchanged — the diff message
// then surfaces the bad value verbatim, which is more useful than
// hiding it.
func normalizeJSON(s string) string {
	var v map[string]any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make(map[string]any, len(v))
	for _, k := range keys {
		out[k] = v[k]
	}
	b, _ := json.Marshal(out)
	return string(b)
}

func unionTopics(a map[string][]mutation.ExpectedProposal, b map[string][]harness.CapturedProposal) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func renderExpected(p mutation.ExpectedProposal) string {
	parts := make([]string, len(p.Entities))
	for i, e := range p.Entities {
		parts[i] = fmt.Sprintf("{key=%q data=%s}", e.Key, mutation.JSONNormalize(e.Data))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func renderActual(p harness.CapturedProposal) string {
	parts := make([]string, len(p.Entities))
	for i, e := range p.Entities {
		parts[i] = fmt.Sprintf("{key=%q data=%s}", e.Key, normalizeJSON(e.Data))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func renderTopicDump(topic string, exp []mutation.ExpectedProposal, act []harness.CapturedProposal) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  topic %q full dump:\n", topic)
	fmt.Fprintf(&b, "  expected (%d):\n", len(exp))
	for i, p := range exp {
		fmt.Fprintf(&b, "    [%d] %s\n", i, renderExpected(p))
	}
	fmt.Fprintf(&b, "  actual (%d):\n", len(act))
	for i, p := range act {
		fmt.Fprintf(&b, "    [%d] %s\n", i, renderActual(p))
	}
	return b.String()
}
