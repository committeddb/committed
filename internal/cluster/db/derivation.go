package db

import (
	"fmt"
	"sort"
	"strings"
)

// DerivationEdge is one syncable config's contribution to the derivation
// graph: every Sources topic feeds every Targets topic. Configs that derive
// no topics (every non-loopback kind) contribute no edge and are never
// refused by these guards.
type DerivationEdge struct {
	ID      string
	Index   uint64 // ordering key: the raft index the config's current version applied at
	Sources []string
	Targets []string
}

// CheckDerivation reports why adding candidate's edges to the accepted graph
// would be refused, or nil. Two invariants, both non-negotiable:
//
//   - The graph stays a DAG. A derivation cycle is an infinite consensus
//     loop (each worker re-proposing the other's output forever), so it must
//     be impossible, not discouraged.
//   - One producer per derived topic. Loopbacks forward their SOURCE's
//     refresh epochs verbatim, so a topic fed by two producers interleaves
//     two unrelated epoch spaces — and one source's reconciling sweep
//     (generation < boundary) could erase the other source's rows on every
//     keyed sink downstream. Refusing fan-in keeps a derived topic's epoch
//     space single-sourced.
func CheckDerivation(accepted []DerivationEdge, candidate DerivationEdge) error {
	if len(candidate.Targets) == 0 {
		return nil
	}
	producers := make(map[string]string) // topic -> producing config id
	adjacency := make(map[string][]string)
	for _, e := range accepted {
		if e.ID == candidate.ID {
			continue // a re-POST replaces the config's own prior edges
		}
		for _, t := range e.Targets {
			producers[t] = e.ID
			for _, s := range e.Sources {
				adjacency[s] = append(adjacency[s], t)
			}
		}
	}

	for _, t := range candidate.Targets {
		if by, taken := producers[t]; taken {
			return fmt.Errorf(
				"derived topic %q already has a producer (syncable %q): a derived topic has exactly one producer, because two would interleave their refresh-epoch spaces and one source's reconciling sweep could erase the other's rows downstream; derive into a separate topic or delete the other syncable first", t, by)
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
// in ascending Index order (the order their current versions landed on the
// log), greedily accepting. The result — which configs are refused — is a
// pure function of the stored config set, so every node (and every restart)
// computes the same answer: the deterministic backstop for configs that
// raced past the leader's admission check.
func ReplayDerivation(edges []DerivationEdge) map[string]error {
	ordered := make([]DerivationEdge, len(edges))
	copy(ordered, edges)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Index < ordered[j].Index })

	refused := make(map[string]error)
	var accepted []DerivationEdge
	for _, e := range ordered {
		if len(e.Targets) == 0 {
			continue
		}
		if err := CheckDerivation(accepted, e); err != nil {
			refused[e.ID] = err
			continue
		}
		accepted = append(accepted, e)
	}
	return refused
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
