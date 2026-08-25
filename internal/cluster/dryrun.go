package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// The dry-run instrument: rehearse a syncable config against a bounded
// sample of the committed log — nothing is admitted, nothing reaches
// raft, nothing writes to a destination — and report what each stage
// and source DID with the sample, in seconds instead of a full replay.
// It exists because the field's dominant failure mode is silent-empty:
// a valid config, a correct-looking table, zero dead letters, and a
// wrong result detectable only by diffing a finished replay against an
// oracle. The report carries the counters that split those states, a
// few sample outputs per stage (derived diagnostic data, under the
// trusted-appliance model), and auto-generated findings that codify
// the diagnostic playbook the field campaign learned by hand.

// DryRunOptions bounds a dry run.
type DryRunOptions struct {
	// MaxEntries caps how many committed Actuals the feed yields across
	// all sampling windows.
	MaxEntries int
	// Timeout bounds the whole run; the deadline produces a TRUNCATED
	// report, never an error (zero = the server default).
	Timeout time.Duration
	// FromIndex, when nonzero, targets ONE window starting at that raft
	// index instead of the default evenly-spaced multi-window sampling
	// — for drilling into a specific log region.
	FromIndex uint64
}

// DryRunFeed yields committed Actuals to fold — the caller (the node)
// owns window selection and budget; the syncable folds what it is fed.
type DryRunFeed func(yield func(*Actual) error) error

// DryRunner is implemented by syncable kinds that can rehearse a
// config. The projection kind implements it; kinds that cannot
// meaningfully dry-run simply don't.
type DryRunner interface {
	DryRun(ctx context.Context, feed DryRunFeed, opts DryRunOptions) (*DryRunReport, error)
}

// ErrDryRunTruncated is the graceful-stop sentinel: the sampling
// deadline arrived mid-feed. Both layers treat it as "stop and report
// what we have" — a partial report beats a timeout error, because the
// partial report carries the phase timings that SAY where the time
// went.
var ErrDryRunTruncated = errors.New("dry-run deadline reached")

// DryRunReport is what the sample revealed.
type DryRunReport struct {
	// Entries is how many committed Actuals were folded.
	Entries    int   `json:"entries"`
	DurationMs int64 `json:"durationMs"`
	// ParseMs is admission-level parsing; ReadMs is log reading and
	// decoding across all windows — with DurationMs, the three split
	// "where did the time go" (the remainder is fold work).
	ParseMs int64 `json:"parseMs"`
	ReadMs  int64 `json:"readMs"`
	// Truncated, when set, says the run stopped mid-sample and why
	// (deadline, or a log read failure) — the counters and findings
	// cover what WAS folded.
	Truncated string `json:"truncated,omitempty"`
	// Coverage is "complete" when the sampled windows collectively read
	// the whole log (first window from 0, every window ending at eof or
	// a boundary, no truncation) — the fact that turns "topic never
	// appeared" from a sampling note into a config fault.
	Coverage string `json:"coverage,omitempty"`
	// TopicsUnseen are CONSUMED topics with zero sampled entities — the
	// structured fact; the node writes the interpretation, because only
	// it knows whether coverage was complete.
	TopicsUnseen []string `json:"topicsUnseen,omitempty"`
	// DeadLetters counts entries whose fold failed PERMANENTLY (an
	// undecodable entity) — the live worker dead-letters these and
	// continues, and the dry-run mirrors it: one bad entity must not
	// abort the rehearsal. Zero here is the same health claim the
	// pilot's replay verdicts lead with.
	DeadLetters int            `json:"deadLetters"`
	Windows     []DryRunWindow `json:"windows,omitempty"`
	// Stages maps stage name → what it did with the sample.
	Stages  map[string]DryRunStage `json:"stages,omitempty"`
	Sources []DryRunSource         `json:"sources,omitempty"`
	// Findings are auto-flagged silent-empty signatures, each with the
	// interpretation and the next move — the campaign's diagnostic
	// playbook, codified. Empty means no signature fired on this sample.
	Findings []string `json:"findings"`
}

// CoverageFindings writes the interpretation of unseen consumed
// topics. With COMPLETE coverage, "never appeared" means the topic has
// no entries anywhere in the log — almost always a mistyped topic id,
// a config fault that partial-coverage wording ("raise maxEntries")
// would misdiagnose as eternal under-sampling.
func CoverageFindings(unseen []string, complete bool) []string {
	if len(unseen) == 0 {
		return nil
	}
	list := strings.Join(unseen, ", ")
	if complete {
		return []string{fmt.Sprintf("consumed topic(s) have NO entries anywhere in the log: %s — check the topic id(s); with complete coverage this is a config fault, not a sampling gap", list)}
	}
	return []string{fmt.Sprintf("coverage: consumed topic(s) never appeared in the sampled windows: %s — their log regions were missed; raise ?maxEntries or target ?fromIndex", list)}
}

// DryRunWindow is one sampled region of the log.
type DryRunWindow struct {
	FromIndex uint64 `json:"fromIndex"`
	Entries   int    `json:"entries"`
	// Ms is wall-clock spent reading this window (resolution + reads).
	Ms int64 `json:"ms"`
	// Stop says why this window ended — "budget", "eof", "boundary",
	// "deadline", or "error" — so an under-filled window is
	// self-describing (the field compared entries against maxEntries to
	// DISCOVER a silent time-bound; every exit path now names itself).
	Stop string `json:"stop,omitempty"`
}

// DryRunStage is one stage's dry-run row: the flow counters, the store
// truth, the delta traffic its consumers saw, per-join resolution, and
// the first few output objects (the value-shaped bugs — null columns,
// wrong paths — are invisible in counts).
type DryRunStage struct {
	Inputs         int64             `json:"inputs"`
	Fanned         int64             `json:"fanned,omitempty"`
	Keys           int               `json:"keys"`
	UnkeyedDeletes int64             `json:"unkeyedDeletes,omitempty"`
	LiveDeltas     int64             `json:"liveDeltas"`
	Retractions    int64             `json:"retractions"`
	Joins          []JoinStat        `json:"joins,omitempty"`
	Samples        []json.RawMessage `json:"samples,omitempty"`
}

// JoinStat is one join's resolution counters, with enough of the
// declaration to read them: a high miss count on an ABSENT join is
// healthy suppression evidence; on a required join it is rejection.
type JoinStat struct {
	Target   string `json:"target"`
	Alias    string `json:"alias,omitempty"`
	Absent   bool   `json:"absent,omitempty"`
	Optional bool   `json:"optional,omitempty"`
	Hits     int64  `json:"hits"`
	Misses   int64  `json:"misses"`
}

// DryRunSource is one table source's matching row. Hints carry the
// value-family evidence for clauses that never matched — the
// equals-"true"-against-a-boolean class, named instead of hunted.
type DryRunSource struct {
	Topic       string   `json:"topic,omitempty"`
	From        string   `json:"from,omitempty"`
	Seen        int64    `json:"seen"`
	Matched     int64    `json:"matched"`
	RuleMatches []int64  `json:"ruleMatches,omitempty"`
	Hints       []string `json:"hints,omitempty"`
}
