package cluster

import (
	"errors"
	"fmt"
	"sync"
)

// ErrConfigShaped marks a failure a site has established as CONFIG-shaped:
// it has recurred across a run of consecutive distinct rows since the site
// last succeeded, so it cannot be entry-specific. It deliberately does NOT
// wrap ErrPermanent — the worker treats it as transient, wedging visibly
// until the operator fixes the config, per the egress classification rule
// (permanent ⟺ entry-specific).
var ErrConfigShaped = errors.New("syncable: config-shaped failure")

// AmbiguityEvidenceThreshold is how many consecutive DISTINCT rows a site
// must fail — with no intervening success of that same site — before the
// failure is established config-shaped. Below it, each failure dead-letters
// as entry-specific (bounded, replayable after the config fix); at it, the
// worker wedges instead of shunting the rest of the topic to dead letters.
// The asymmetric risk picks both the direction and the low threshold: a
// wrongful wedge is loud and fully recoverable; a wrongful dead-letter run
// silently drops delivered data.
const AmbiguityEvidenceThreshold = 10

// AmbiguityTracker classifies an AMBIGUOUSLY-permanent failure site — one
// whose failure carries no information distinguishing entry-specific (a
// field genuinely absent in THIS row, a row THIS program can't transform →
// permanent is right) from config-shaped (a wrong-for-the-whole-topic
// jsonpath typo, a broken migration program → transient is right). One
// evaluation cannot tell them apart; the site's own history can: an
// entry-specific fault does not recur across every distinct row the site
// evaluates, and a config-shaped one never lets the site succeed.
//
// Each configured extraction/transform site holds one tracker. On a
// successful evaluation the site calls Succeeded; on a failure it returns
// Classify's result instead of wrapping Permanent itself. The counter is
// per-site, so successes of OTHER rows or OTHER paths never mask a path
// that fails every row it is actually evaluated against (a typo inside a
// when-gated rule on a mixed topic), and a site that stops matching after
// a schema drift re-accumulates evidence and wedges rather than
// dead-lettering to the breaker park.
//
// State is in-memory and node-local, like the sync breaker's: a restart or
// leadership move re-counts, costing at most threshold-1 extra replayable
// dead letters. A nil tracker classifies every failure Permanent — the safe
// default for directly-constructed configs that skipped parse wiring.
type AmbiguityTracker struct {
	mu sync.Mutex
	// lastRow dedups retries: the worker re-presents a failed Actual, and a
	// retry of the same row is not new evidence. Distinctness is keyed on
	// the Actual's Index (entity keys may be empty on unkeyed topics).
	lastRow uint64
	hasLast bool
	// consecutive counts distinct-row failures since the site's last
	// success.
	consecutive int
}

// NewAmbiguityTracker returns a tracker for one configured site.
func NewAmbiguityTracker() *AmbiguityTracker { return &AmbiguityTracker{} }

// Succeeded records that the site's evaluation matched a row, resetting the
// evidence run — subsequent failures are entry-specific until a fresh run
// accumulates.
func (t *AmbiguityTracker) Succeeded() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.consecutive = 0
	t.hasLast = false
}

// Classify records a failure of the site on the row committed at index and
// returns err classified per the accumulated evidence: Permanent while the
// failure may still be entry-specific, ErrConfigShaped (transient — the
// worker wedges) once the run establishes it config-shaped. The wedge
// retries the threshold row, which keeps the run at the threshold — the
// classification is stable until the operator's fix lets the site succeed.
func (t *AmbiguityTracker) Classify(index uint64, err error) error {
	if t == nil {
		return Permanent(err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.hasLast || t.lastRow != index {
		t.consecutive++
		t.lastRow = index
		t.hasLast = true
	}
	if t.consecutive >= AmbiguityEvidenceThreshold {
		return fmt.Errorf("%w: failed %d consecutive distinct rows with no success at this site — an entry-specific fault does not recur across every row, so this is config-shaped; the worker retries (wedged) until the config is fixed: %w",
			ErrConfigShaped, t.consecutive, err)
	}
	return Permanent(err)
}

// AmbiguityTrackers is one tracker per configured path, positionally aligned
// with the path slice it accompanies.
type AmbiguityTrackers []*AmbiguityTracker

// NewAmbiguityTrackers allocates one tracker per site.
func NewAmbiguityTrackers(n int) AmbiguityTrackers {
	out := make(AmbiguityTrackers, n)
	for i := range out {
		out[i] = NewAmbiguityTracker()
	}
	return out
}

// At returns the i-th tracker, or nil (which classifies Permanent) when the
// slice was never wired or is shorter than its path slice — the safe default
// for directly-constructed configs.
func (ts AmbiguityTrackers) At(i int) *AmbiguityTracker {
	if i < 0 || i >= len(ts) {
		return nil
	}
	return ts[i]
}
