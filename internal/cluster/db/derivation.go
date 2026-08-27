package db

import (
	"fmt"
	"sort"
	"strings"
)

// DerivationEdge is one config's contribution to the producer graph: every
// Sources topic feeds every Targets topic. Two kinds contribute edges — a
// deriving syncable (a loopback: sources → targets) and an ingestable (no
// sources; its topics are targets it mints refresh epochs for). Configs that
// produce no topics (every non-loopback syncable kind) contribute no edge
// and are never refused by these guards.
type DerivationEdge struct {
	// Kind is the config namespace ("syncable" or "ingestable") — the same
	// strings the degraded-config records use. Kind+ID identifies a config;
	// the two namespaces may reuse an ID.
	Kind    string
	ID      string
	Index   uint64 // ordering key: the raft index the config's current version applied at
	Sources []string
	Targets []string
}

// EdgeRef names one edge-contributing config across both namespaces.
type EdgeRef struct {
	Kind string
	ID   string
}

// CheckDerivation reports why adding candidate's edges to the accepted graph
// would be refused, or nil. Two invariants, both non-negotiable:
//
//   - The graph stays a DAG. A derivation cycle is an infinite consensus
//     loop (each worker re-proposing the other's output forever), so it must
//     be impossible, not discouraged.
//   - One epoch-stamping producer per topic. An ingestable mints its topics'
//     refresh epochs and a loopback forwards its SOURCE's epochs verbatim,
//     so a topic fed by two producers — in any kind combination —
//     interleaves two unrelated epoch spaces, and one producer's reconciling
//     sweep (generation < boundary) could erase the other's rows on every
//     keyed sink downstream. Refusing fan-in keeps every produced topic's
//     epoch space single-sourced. (Direct user proposals carry Generation 0,
//     which is never swept — not part of this class.)
func CheckDerivation(accepted []DerivationEdge, candidate DerivationEdge) error {
	if len(candidate.Targets) == 0 {
		return nil
	}
	producers := make(map[string]EdgeRef) // topic -> producing config
	adjacency := make(map[string][]string)
	for _, e := range accepted {
		if e.Kind == candidate.Kind && e.ID == candidate.ID {
			continue // a re-POST replaces the config's own prior edges
		}
		for _, t := range e.Targets {
			producers[t] = EdgeRef{Kind: e.Kind, ID: e.ID}
			for _, s := range e.Sources {
				adjacency[s] = append(adjacency[s], t)
			}
		}
	}

	for _, t := range candidate.Targets {
		if by, taken := producers[t]; taken {
			return fmt.Errorf(
				"topic %q already has a producer (%s %q): a topic has exactly one epoch-stamping producer, because two would interleave their refresh-epoch spaces and one producer's reconciling sweep could erase the other's rows on every keyed sink downstream; use a separate topic or delete the other config first", t, by.Kind, by.ID)
		}
	}

	// Add the candidate's edges, then look for a path target ↝ source: with
	// the candidate's source → target edge that path closes a cycle.
	for _, t := range candidate.Targets {
		for _, s := range candidate.Sources {
			adjacency[s] = append(adjacency[s], t)
		}
	}
	for _, s := range candidate.Sources {
		for _, t := range candidate.Targets {
			if path := findPath(adjacency, t, s); path != nil {
				cycle := append([]string{s}, path...)
				return fmt.Errorf(
					"derivation cycle: %s — a cycle would re-derive its own output forever (an infinite consensus loop); break the chain", strings.Join(cycle, " → "))
			}
		}
	}
	return nil
}

// ReplayDerivation applies CheckDerivation to every edge-contributing config
// — both kinds, jointly — in ascending Index order (the order their current
// versions landed on the log), greedily accepting. The result — which
// configs are refused — is a pure function of the stored config set, so
// every node (and every restart) computes the same answer: the deterministic
// backstop for configs that raced past the leader's admission check.
// First-by-log-index wins; a loser is persisted but degraded (no worker).
// Because the verdict is recomputed from the stored set, deleting the winner
// deterministically un-refuses the loser at its next build — safe, since a
// topic's refresh epoch is topic-keyed and survives producer changes, so a
// promoted producer continues the same monotonic epoch space.
func ReplayDerivation(edges []DerivationEdge) map[EdgeRef]error {
	ordered := make([]DerivationEdge, len(edges))
	copy(ordered, edges)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Index < ordered[j].Index })

	refused := make(map[EdgeRef]error)
	var accepted []DerivationEdge
	for _, e := range ordered {
		if len(e.Targets) == 0 {
			continue
		}
		if err := CheckDerivation(accepted, e); err != nil {
			refused[EdgeRef{Kind: e.Kind, ID: e.ID}] = err
			continue
		}
		accepted = append(accepted, e)
	}
	return refused
}

// ReplayWithCandidate answers admission's question with the SAME predicate
// the build path applies: replace the candidate's own stored edges (a
// re-POST replaces a config's claims), append the candidate as the newest
// claimant, replay, and return the candidate's refusal (nil = admissible).
// Because admission and build share one predicate, they cannot skew — what
// admission accepts, the build accepts, on every node.
//
// Ordering subtlety, written down because admission and the build replay
// are load-bearing for each other here: ReplayDerivation orders by
// CURRENT-version index, so an edited config becomes the YOUNGEST claimant
// — seniority is last-edited, not first-created. That would let a lingering
// refused loser take a topic from a running producer the moment the
// producer is re-POSTed (loser@12 < winner@50). It stays unreachable ONLY
// because admission replays against the STORED set — refused losers'
// claims included — so the winner's re-POST is refused ("delete the other
// config first") before a flipped ordering can ever commit. Do not
// "improve" admission to ignore refused configs' claims without also
// changing the ordering key. Accepted residual: a running producer's
// re-POST racing a brand-new claimant's create can resolve for the
// newcomer (deterministic, loud, epoch-safe — the topic-keyed refresh
// epoch survives the handover). Per-(config, topic) first-claim seniority
// was rejected: claims can change across versions, which would force a
// version-history walk into the replay.
func ReplayWithCandidate(stored []DerivationEdge, candidate DerivationEdge) error {
	edges := make([]DerivationEdge, 0, len(stored)+1)
	var maxIndex uint64
	for _, e := range stored {
		if e.Index > maxIndex {
			maxIndex = e.Index
		}
		if e.Kind == candidate.Kind && e.ID == candidate.ID {
			continue // a re-POST replaces the config's own claims
		}
		edges = append(edges, e)
	}
	candidate.Index = maxIndex + 1
	edges = append(edges, candidate)
	return ReplayDerivation(edges)[EdgeRef{Kind: candidate.Kind, ID: candidate.ID}]
}

// findPath returns the topic path from `from` to `to` (inclusive of both), or
// nil when unreachable. Plain DFS with parent tracking — derivation graphs
// are operator-config-sized.
func findPath(adjacency map[string][]string, from, to string) []string {
	parent := map[string]string{from: from}
	stack := []string{from}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n == to {
			var path []string
			for at := to; ; at = parent[at] {
				path = append([]string{at}, path...)
				if at == from {
					return path
				}
			}
		}
		for _, next := range adjacency[n] {
			if _, seen := parent[next]; seen {
				continue
			}
			parent[next] = n
			stack = append(stack, next)
		}
	}
	return nil
}
