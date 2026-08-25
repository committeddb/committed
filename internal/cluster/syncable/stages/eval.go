package stages

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/PaesslerAG/jsonpath"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"
)

// The stage evaluator: the steady-state per-key refold over the stage
// store. Every stage is the same machine — an input arrives (a topic
// entity, or an upstream stage's output), lands in the key's retained
// input set, and the key REFOLDS from that set: never delta arithmetic,
// always recompute-from-inputs, so redelivery, rekeying, retraction, and
// replay all converge to the same bytes (log-prefix determinism).
//
// Identity model (the design note the store's src bucket exists for):
//   - inKey  — the input's identity: a topic entity's Key, or the
//     producing stage's output key.
//   - outKey — the stage's key for that input: keyPath over the input
//     object, canonically rendered.
//   - src: inKey → outKey answers deletes (a tombstone carries only
//     inKey) and rekeys (a re-emitted input whose keyPath moved must
//     retract from its OLD key's fold).
//
// A stage's output object marshals with sorted keys (Go map marshal) and
// exact numbers (json.Number in, FormatRat out), so equal folds are
// byte-equal — the determinism the self-healing arguments lean on.

// graphNode is one stage's runtime: its definition and the downstream
// stages consuming its output.
type graphNode struct {
	def       *Stage
	consumers []*graphNode
	// fanNonArrayRun counts CONSECUTIVE inputs whose forEach path
	// resolved to a PRESENT non-array (worker goroutine only — no lock).
	// At stageForEachNonArrayWarnRun it triggers the one-shot
	// misconfiguration warning; any array-shaped input resets it.
	fanNonArrayRun int
	// inputs/fanned are flow counters for introspection (StageStat):
	// upsert deliveries to this stage, and elements its fan produced.
	// Atomic — folds run on the worker goroutine, the status endpoint
	// reads from an HTTP goroutine.
	inputs atomic.Int64
	fanned atomic.Int64
	// unkeyedDeletes counts delete-shaped inputs (matched deleteWhen)
	// whose key could not resolve or render — LOST RETRACTIONS: the key
	// each meant to kill stays live, silently, with zero dead letters.
	// Nonzero here splits "deletion never captured" from "deletion
	// captured but could not key" in one StageStats read. Same
	// atomicity contract as inputs/fanned.
	unkeyedDeletes atomic.Int64
	// unkeyedDeleteWarned one-shots the warn log (worker goroutine only).
	unkeyedDeleteWarned bool
	// joinFlows counts each join's dimension-resolution outcomes since
	// this worker started — hit = a row matched (passing the join's
	// where) — indexed parallel to def.Joins. With inputs/fanned these
	// split "a join never resolved" out of "the when rejected
	// everything". Required joins count during qualification, optional
	// ones during lookup. Same atomicity contract as inputs/fanned.
	joinFlows []joinFlowCount
	// dimConsumers are the joins (of LATER stages) that use THIS stage's
	// outputs as their dimension rows — maintained by refoldKey as the
	// outputs change, fan-out marking their dependents dirty.
	dimConsumers []dimRef
	// mergeConsumers are the merge stages combining THIS stage's outputs
	// as one aliased side of their tuples.
	mergeConsumers []mergeRef
}

// Graph is the compiled stage topology of one projection: manifest
// order is the DAG (validated), so cascades simply recurse downstream.
// dimRef addresses one join of one stage — a topic's entities feed it as
// dimension rows.
// mergeRef is one merge stage consuming a producer's outputs under an
// alias.
type mergeRef struct {
	node  *graphNode
	alias string
}

type joinFlowCount struct{ hits, misses atomic.Int64 }

type dimRef struct {
	node *graphNode
	join *Join
}

type Graph struct {
	byName  map[string]*graphNode
	byTopic map[string][]*graphNode
	// order is manifest order — the validated topology, and Drain's
	// processing order: a stage's consumers always drain after it.
	order []*graphNode
	// dims routes topics consumed as JOIN DIMENSIONS: rows stored by
	// entity key, dependents refolded on change (reverse-index fan-out).
	dims map[string][]dimRef
	// OnDelta, when set, observes every stage's post-refold delta — the
	// chaining terminal's feed (table sources consuming a stage). live=false
	// is a retraction. Set per Sync call by the single worker goroutine.
	OnDelta func(stage string, outKey []byte, obj any, live bool) error
}

// BuildGraph compiles the validated stage list into its runtime
// topology.
func BuildGraph(stages []Stage) *Graph {
	stages = expandSynthetic(stages)
	g := &Graph{byName: map[string]*graphNode{}, byTopic: map[string][]*graphNode{}, dims: map[string][]dimRef{}}
	nodes := make([]*graphNode, len(stages))
	// The graph owns a copy of the stage definitions (joins deep-copied):
	// stage-join normalize is RESOLVED below by inheritance, and that
	// resolution must never write into the caller's config (which feeds
	// the store fingerprint).
	owned := make([]Stage, len(stages))
	copy(owned, stages)
	for i := range owned {
		owned[i].Joins = append([]Join(nil), owned[i].Joins...)
		nodes[i] = &graphNode{def: &owned[i], joinFlows: make([]joinFlowCount, len(owned[i].Joins))}
		g.byName[owned[i].Name] = nodes[i]
	}
	g.order = nodes
	for i := range owned {
		st := &owned[i]
		if len(st.Merge) > 0 {
			// Aliases resolve here on the graph-owned copy (defaults =
			// stage names, validated path-safe at admission); the merge
			// declares no From and no joins.
			for e := range st.Merge {
				if st.Merge[e].As == "" {
					st.Merge[e].As = st.Merge[e].Stage
				}
				if producer, ok := g.byName[st.Merge[e].Stage]; ok {
					producer.mergeConsumers = append(producer.mergeConsumers, mergeRef{node: nodes[i], alias: st.Merge[e].As})
				}
			}
			continue
		}
		if up, ok := g.byName[st.From]; ok {
			up.consumers = append(up.consumers, nodes[i])
		} else {
			g.byTopic[st.From] = append(g.byTopic[st.From], nodes[i])
		}
		for j := range st.Joins {
			jn := &st.Joins[j]
			if jn.From != "" {
				if producer, ok := g.byName[jn.From]; ok {
					// A stage join INHERITS the joined stage's key space:
					// the dimension keys are the producer's outputs, so the
					// join's references must render in that space — the
					// field defect was an UPPERCASE CDC reference against a
					// lowered stage key, silently never matching (an
					// anti-join that suppressed nothing). Admission rejects
					// a normalize declared on the join itself, so this
					// resolution is the single source of the rendering —
					// and it resolves through ResolvedKeySpace, because a
					// MERGE producer declares no space of its own: reading
					// its fields directly was that defect's merge-fronted
					// twin (an inherited "" against the merge's lowered
					// adopted keys).
					_, kt, norm := ResolvedKeySpace(owned, IndexOf(owned, jn.From))
					jn.Normalize = norm
					jn.OnType = kt
					producer.dimConsumers = append(producer.dimConsumers, dimRef{node: nodes[i], join: jn})
				}
				continue
			}
			g.dims[jn.Topic] = append(g.dims[jn.Topic], dimRef{node: nodes[i], join: jn})
		}
	}
	return g
}

// expandSynthetic rewrites field-addressed joins and topic merge sides
// into the hidden identity re-key/lift stages authors used to write by
// hand — one mechanism, machine-written: each synthetic stage is an
// identity reshape (emits its input verbatim) keyed by the addressed
// field or the declared topic key, inserted in manifest order just
// before its consumer. All existing machinery — retention, dims,
// cascades, retraction, counters — applies unchanged, so the runtime
// cost equals the manual stage's (and future fusion makes both free).
// Synthetic names use the reserved bracket form ("<from>[<path>]");
// stats and dry-run reports show them transparently.
func expandSynthetic(sts []Stage) []Stage {
	out := make([]Stage, 0, len(sts))
	seen := map[string]bool{}
	for i := range sts {
		st := sts[i]
		var pre []Stage
		if len(st.Joins) > 0 {
			st.Joins = append([]Join(nil), st.Joins...)
			for j := range st.Joins {
				jn := &st.Joins[j]
				if jn.Field == "" {
					continue
				}
				name := jn.From + "[" + jn.Field + "]"
				if !seen[name] {
					seen[name] = true
					pre = append(pre, Stage{
						Name:         name,
						From:         jn.From,
						KeyPath:      []string{jn.Field},
						KeyType:      jn.OnType,
						Normalize:    jn.Normalize,
						identityEmit: true,
					})
				}
				jn.From = name
				jn.Field = ""
				// The rewritten join inherits the synthetic stage's
				// space through the normal resolution below.
				jn.Normalize = ""
				jn.OnType = nil
			}
		}
		if len(st.Merge) > 0 {
			st.Merge = append([]MergeEntry(nil), st.Merge...)
			for e := range st.Merge {
				me := &st.Merge[e]
				if me.Topic == "" {
					continue
				}
				name := me.Topic + "[" + strings.Join(me.KeyPath, ",") + "]"
				if !seen[name] {
					seen[name] = true
					pre = append(pre, Stage{
						Name:         name,
						From:         me.Topic,
						KeyPath:      me.KeyPath,
						KeyType:      me.KeyType,
						Normalize:    me.Normalize,
						identityEmit: true,
					})
				}
				if me.As == "" {
					me.As = me.Topic
				}
				me.Stage = name
				me.Topic = ""
				me.KeyPath, me.KeyType, me.Normalize = nil, nil, ""
			}
		}
		out = append(out, pre...)
		out = append(out, st)
	}
	return out
}

// ConsumesTopic reports whether any stage reads this topic — as its
// input or as a join dimension.
func (g *Graph) ConsumesTopic(topic string) bool {
	return len(g.byTopic[topic]) > 0 || len(g.dims[topic]) > 0
}

// SweepEpochs applies a refresh-boundary marker for one topic: retained
// inputs and dimension rows captured under an OLDER refresh epoch
// (1 <= gen < marker) were not re-asserted by the re-snapshot — the
// source deleted them in the lost window — so they retract, and every
// affected key refolds through the drain as EXPLICIT deltas. Generation
// 0 (direct writes, stage-fed retention) is never swept: the >= 1 floor,
// and the absorption rule — downstream stages see deltas, never sweeps.
func (g *Graph) SweepEpochs(tx *stagestore.Tx, topic string, marker uint64, dirty Dirty) error {
	for _, n := range g.byTopic[topic] {
		st := n.def
		type stale struct{ outKey, inKey []byte }
		var stales []stale
		if err := tx.InputsAll(st.Name, func(outKey, inKey, val []byte) error {
			if gen, _ := decodeRetained(val); gen >= 1 && gen < marker {
				stales = append(stales, stale{append([]byte(nil), outKey...), append([]byte(nil), inKey...)})
			}
			return nil
		}); err != nil {
			return err
		}
		for _, sl := range stales {
			if err := tx.DeleteIn(st.Name, sl.outKey, sl.inKey); err != nil {
				return err
			}
			if err := tx.DeleteSrc(st.Name, sl.inKey); err != nil {
				return err
			}
			dirty.Mark(n, sl.outKey)
		}
	}
	for _, d := range g.dims[topic] {
		var staleDims [][]byte
		if err := tx.DimsAll(d.node.def.Name, d.join.Topic, func(dimKey, val []byte) error {
			if gen, _ := decodeRetained(val); gen >= 1 && gen < marker {
				staleDims = append(staleDims, append([]byte(nil), dimKey...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, dk := range staleDims {
			if err := tx.DeleteDim(d.node.def.Name, d.join.Topic, dk); err != nil {
				return err
			}
			if err := g.markDimDependents(tx, d, dk, dirty); err != nil {
				return err
			}
		}
	}
	return nil
}

// Dirty collects the keys each stage must refold — the set-at-a-time
// half of the two execution modes: retention updates mark keys dirty,
// and Drain refolds each dirty key ONCE per batch (the 87-minutes-vs-
// 5-seconds field lesson), suppressing cascades whose output bytes did
// not change (sound because folds are deterministic).
type Dirty map[*graphNode]map[string]bool

func (d Dirty) Mark(n *graphNode, outKey []byte) {
	m, ok := d[n]
	if !ok {
		m = map[string]bool{}
		d[n] = m
	}
	m[string(outKey)] = true
}

// FoldTopicUpsertNow folds one entity and drains immediately — the
// single-event convenience (tests; the projection batches per Actual).
func (g *Graph) FoldTopicUpsertNow(tx *stagestore.Tx, topic string, inKey []byte, payload any) error {
	dirty := Dirty{}
	if err := g.FoldTopicUpsert(tx, topic, inKey, payload, 0, dirty); err != nil {
		return err
	}
	return g.Drain(tx, dirty)
}

// FoldTopicDeleteNow is FoldTopicUpsertNow's tombstone twin.
func (g *Graph) FoldTopicDeleteNow(tx *stagestore.Tx, topic string, inKey []byte) error {
	dirty := Dirty{}
	if err := g.FoldTopicDelete(tx, topic, inKey, dirty); err != nil {
		return err
	}
	return g.Drain(tx, dirty)
}

// FoldTopicUpsert routes one topic entity's upsert through every stage
// consuming that topic (and their downstream chains).
func (g *Graph) FoldTopicUpsert(tx *stagestore.Tx, topic string, inKey []byte, payload any, gen uint64, dirty Dirty) error {
	for _, n := range g.byTopic[topic] {
		if err := g.foldUpsert(tx, n, inKey, payload, gen, dirty); err != nil {
			return err
		}
	}
	for _, d := range g.dims[topic] {
		bs, err := marshalStageObject(payload)
		if err != nil {
			return err
		}
		// The join's normalize covers the DIMENSION side too: the topic's
		// entity-key rendering is the producer's (an UPPERCASE CDC GUID),
		// and only this join knows both sides must agree.
		dimKey := []byte(NormalizeKeyPart(d.join.Normalize, string(inKey)))
		if err := tx.PutDim(d.node.def.Name, d.join.target(), dimKey, encodeRetained(gen, bs)); err != nil {
			return err
		}
		if err := g.markDimDependents(tx, d, dimKey, dirty); err != nil {
			return err
		}
	}
	return nil
}

// FoldTopicDelete routes one topic tombstone (key only, no payload).
func (g *Graph) FoldTopicDelete(tx *stagestore.Tx, topic string, inKey []byte, dirty Dirty) error {
	for _, n := range g.byTopic[topic] {
		if err := g.foldDelete(tx, n, inKey, dirty); err != nil {
			return err
		}
	}
	for _, d := range g.dims[topic] {
		dimKey := []byte(NormalizeKeyPart(d.join.Normalize, string(inKey)))
		if err := tx.DeleteDim(d.node.def.Name, d.join.target(), dimKey); err != nil {
			return err
		}
		if err := g.markDimDependents(tx, d, dimKey, dirty); err != nil {
			return err
		}
	}
	return nil
}

// markDimDependents marks every key whose inputs reference this
// dimension row (the fan-out) for refold at Drain. Reverse-index entries
// can be stale (an input rekeyed or retracted since registering) — a
// stale entry's refold is an idempotent no-op, never wrong output; a
// rebuild clears them.
func (g *Graph) markDimDependents(tx *stagestore.Tx, d dimRef, dimKey []byte, dirty Dirty) error {
	return tx.DependentsOf(d.node.def.Name, d.join.target(), dimKey, func(outKey []byte) error {
		dirty.Mark(d.node, outKey)
		return nil
	})
}

// foldUpsert lands one input in a stage: filter, key, retain, refold the
// affected key(s), cascade the delta. An input that stops matching the
// stage's when RETRACTS (filtering is refold, not skip).
func (g *Graph) foldUpsert(tx *stagestore.Tx, n *graphNode, inKey []byte, payload any, gen uint64, dirty Dirty) error {
	n.inputs.Add(1)
	if n.def.hasFan() {
		return g.foldFan(tx, n, inKey, payload, gen, dirty)
	}
	return g.foldOneInput(tx, n, inKey, payload, nil, gen, dirty)
}

// FoldActual folds one committed Actual's entities into the graph —
// the shared ingress of the live worker, the recovery pass, and the
// dry-run (one loop, so the three can never diverge): upserts and
// tombstones route to consuming stages, refresh markers sweep epochs,
// and the drain refolds every dirtied key. The caller owns frontier
// bookkeeping.
func (g *Graph) FoldActual(tx *stagestore.Tx, a *cluster.Actual) error {
	dirty := Dirty{}
	for _, e := range a.Entities {
		if !g.ConsumesTopic(e.Type.ID) {
			continue
		}
		switch e.Variant() {
		case cluster.EntityVariantDelete:
			if err := g.FoldTopicDelete(tx, e.Type.ID, e.Key, dirty); err != nil {
				return err
			}
		case cluster.EntityVariantRow:
			obj, err := DecodeObject(e.Data)
			if err != nil {
				return cluster.Permanent(fmt.Errorf("[projection.stage] unmarshal entity data: %w", err))
			}
			if err := g.FoldTopicUpsert(tx, e.Type.ID, e.Key, obj, e.Generation, dirty); err != nil {
				return err
			}
		case cluster.EntityVariantRefresh:
			// The epoch sweep: inputs and dimension rows this re-snapshot
			// did not re-assert retract, refolding their keys as explicit
			// deltas — downstream (including stage-fed table sources)
			// never needs sweep semantics of its own.
			if err := g.SweepEpochs(tx, e.Type.ID, e.Generation, dirty); err != nil {
				return err
			}
		default:
			// Future variants fold nothing here; the source-side apply
			// dead-letters them loudly.
		}
	}
	return g.Drain(tx, dirty)
}

// fanPathDesc names the fan declaration for logs: the single forEach
// path, or the arm paths joined.
func fanPathDesc(st *Stage) string {
	if st.ForEach != "" {
		return st.ForEach
	}
	parts := make([]string, len(st.Fan))
	for i := range st.Fan {
		parts[i] = st.Fan[i].ForEach
	}
	return strings.Join(parts, " | ")
}

// FanMiss resolves a forEach path and reports why the fan is empty ("" =
// healthy). The subtlety making this a function: the jsonpath library's
// wildcard returns an EMPTY match set — no error — when the path
// traverses into a non-container (the field case: a serialized-JSON
// string column), which is indistinguishable at the type level from a
// legitimately empty array. When the fan comes up empty, the CONTAINER
// (the path minus its trailing [*]) is probed: an array there is a
// healthy empty list; a string, other scalar, or unresolvable path is a
// miss. Multi-valued forms without a trailing [*] skip the probe (no
// false warns; the [*] suffix is both field cases and the documented
// shape).
func FanMiss(path string, payload any, elems *[]any) string {
	miss := "unresolved"
	if v, err := jsonpath.Get(path, payload); err == nil {
		if list, ok := v.([]any); ok {
			*elems = list
			miss = ""
		} else {
			miss = fmt.Sprintf("%T", v)
		}
	}
	if miss == "" && len(*elems) == 0 {
		if container, ok := strings.CutSuffix(path, "[*]"); ok {
			if cv, err := jsonpath.Get(container, payload); err != nil {
				miss = "unresolved"
			} else if _, isList := cv.([]any); !isList {
				miss = fmt.Sprintf("%T", cv)
			}
		}
	}
	return miss
}

// FlowCounts reports each stage's in-memory flow counters (upsert
// deliveries; fanned elements for forEach stages) since this graph was
// built — the introspection halves that, with the store's key counts,
// split "region not reached" from "fan empty" from "filtered to zero".
func (g *Graph) FlowCounts() map[string]FlowCount {
	out := make(map[string]FlowCount, len(g.order))
	for _, n := range g.order {
		fc := FlowCount{Inputs: n.inputs.Load(), Fanned: n.fanned.Load(), UnkeyedDeletes: n.unkeyedDeletes.Load()}
		for ji := range n.def.Joins {
			j := &n.def.Joins[ji]
			target := j.Topic
			if target == "" {
				target = j.From
			}
			fc.Joins = append(fc.Joins, JoinFlow{Target: target, Alias: j.As, Absent: j.Absent, Optional: j.Optional, Hits: n.joinFlows[ji].hits.Load(), Misses: n.joinFlows[ji].misses.Load()})
		}
		out[n.def.Name] = fc
	}
	return out
}

// FlowCount is one stage's flow counters (see Graph.FlowCounts).
type FlowCount struct {
	Inputs int64
	Fanned int64
	// UnkeyedDeletes counts lost retractions — see graphNode.
	UnkeyedDeletes int64
	// Joins are the per-join resolution counters — see graphNode.
	Joins []JoinFlow
}

// JoinFlow is one join's resolution counters plus enough declaration to
// interpret them (a high miss count on an ABSENT join is healthy
// suppression evidence; on a required join it is rejection).
type JoinFlow struct {
	Target   string
	Alias    string
	Absent   bool
	Optional bool
	Hits     int64
	Misses   int64
}

// stageForEachNonArrayWarnRun is how many CONSECUTIVE non-array forEach
// resolutions a stage absorbs before warning that its fan is empty —
// the same threshold rationale as the sink's rules-unmatched run.
const stageForEachNonArrayWarnRun = 1000

// foldFan fans one input through a forEach stage: each element folds as
// its own input — identity (parent key, element key) — and the fan
// reverse-index reconciles: elements the re-emitted input no longer
// carries retract, and the input's tombstone retracts them all.
func (g *Graph) foldFan(tx *stagestore.Tx, n *graphNode, parentKey []byte, payload any, gen uint64, dirty Dirty) error {
	st := n.def
	type armFan struct {
		ordinal int // -1 = single-path forEach (legacy identity bytes)
		elems   []any
	}
	var fans []armFan
	miss := ""
	if len(st.Fan) > 0 {
		// Multi-arm: every arm whose when matches this INPUT fans its
		// path (UNION ALL). An input matching no arm is healthy
		// foreignness; a matching arm resolving no array counts toward
		// the non-array warn run like a single-path miss.
		anyMatched, anyHealthy := false, false
		lastMiss := ""
		for ai := range st.Fan {
			arm := &st.Fan[ai]
			if !MatchScoped(arm.When, payload, nil) {
				continue
			}
			anyMatched = true
			var elems []any
			if m := FanMiss(arm.ForEach, payload, &elems); m == "" {
				anyHealthy = true
			} else {
				lastMiss = m
			}
			fans = append(fans, armFan{ai, elems})
		}
		if anyMatched && !anyHealthy {
			miss = lastMiss
		}
		if !anyMatched {
			return nil
		}
	} else {
		var elems []any
		miss = FanMiss(st.ForEach, payload, &elems)
		fans = append(fans, armFan{-1, elems})
	}
	if miss != "" {
		// A forEach path yielding no array fans ZERO elements — correct
		// for the odd foreign event, catastrophic as a steady state: the
		// field case was a serialized-JSON string column, where the path
		// traverses INTO the string and errors on every input, producing
		// two full 36M-entry replays of structurally plausible, silently
		// empty stage output that only an oracle diff caught. Same
		// antidote as the sink's rules-unmatched run: count consecutive
		// misses, warn once with the probable cause and the remedy. An
		// empty array is a healthy resolution and resets the run.
		n.fanNonArrayRun++
		if n.fanNonArrayRun == stageForEachNonArrayWarnRun {
			zap.L().Warn("[stage] forEach path has not resolved to an array for a long run of inputs — this stage is fanning ZERO elements and its output is silently empty; if the field is a serialized-JSON column at the source, decode it at ingest (jsonColumns) so the path reaches a real array",
				zap.String("stage", st.Name),
				zap.String("forEach", fanPathDesc(st)),
				zap.String("value_type", miss),
				zap.Int("consecutive_misses", n.fanNonArrayRun))
		}
	} else {
		n.fanNonArrayRun = 0
	}

	prior := map[string]bool{}
	if err := tx.DependentsOf(st.Name, fanJoinName, parentKey, func(elemIn []byte) error {
		prior[string(elemIn)] = true
		return nil
	}); err != nil {
		return err
	}

	identityPath := st.ElementKey
	if identityPath == "" {
		identityPath = st.KeyPath[0]
	}
	current := map[string]bool{}
	for _, f := range fans {
		for _, el := range f.elems {
			n.fanned.Add(1)
			kv, err := ResolvePath(identityPath, el, payload)
			if err != nil || kv == nil {
				continue // an unidentifiable element fans nothing (and reconciles away)
			}
			ek := []byte(KeyString(kv))
			if f.ordinal >= 0 {
				ek = fanArmElemKey(f.ordinal, ek)
			}
			elemIn := fanInKey(parentKey, ek)
			if current[string(elemIn)] {
				continue
			}
			current[string(elemIn)] = true
			if err := g.foldOneInput(tx, n, elemIn, el, payload, gen, dirty); err != nil {
				return err
			}
			if err := tx.PutRev(st.Name, fanJoinName, parentKey, elemIn); err != nil {
				return err
			}
		}
	}
	for pk := range prior {
		if current[pk] {
			continue
		}
		if err := g.foldDeleteInput(tx, n, []byte(pk), dirty); err != nil {
			return err
		}
		if err := tx.DeleteRev(st.Name, fanJoinName, parentKey, []byte(pk)); err != nil {
			return err
		}
	}
	return nil
}

// foldOneInput lands one input (an entity, an upstream delta, or a
// fanned element with its parent in scope) in a stage's retained set.
func (g *Graph) foldOneInput(tx *stagestore.Tx, n *graphNode, inKey []byte, data, parent any, gen uint64, dirty Dirty) error {
	st := n.def
	deleteShaped := st.Reduce == "liveSet" && MatchScoped(st.DeleteWhen, data, parent)
	if !deleteShaped && !MatchScoped(st.When, data, parent) {
		return g.foldDeleteInput(tx, n, inKey, dirty)
	}

	parts := make([]string, len(st.KeyPath))
	for i, kp := range st.KeyPath {
		kv, err := ResolvePath(kp, data, parent)
		if err != nil || kv == nil {
			// A matched input without a (complete) key cannot fold; treat as
			// non-membership (and retract any prior membership) rather than
			// erroring the topic. Delete evidence dropped here is a LOST
			// retraction — made loud, since the key it meant to kill stays
			// live.
			if deleteShaped {
				g.noteUnkeyedDelete(n, kp)
			}
			return g.foldDeleteInput(tx, n, inKey, dirty)
		}
		part, ok := TypedKeyPart(KeyTypeAt(st.KeyType, i), coerceKeyScalar(kv))
		if !ok {
			// The value cannot render into its declared comparison space
			// (a non-numeric string under keyType "number") — same
			// non-membership as a missing key part, and the same lost
			// retraction when the input was delete-shaped.
			if deleteShaped {
				g.noteUnkeyedDelete(n, kp)
			}
			return g.foldDeleteInput(tx, n, inKey, dirty)
		}
		parts[i] = NormalizeKeyPart(st.Normalize, part)
	}
	outKey := []byte(OutKey(parts))

	// Rekey: if this input previously fed a DIFFERENT key, retract it
	// there first — its old key refolds without it.
	prior, err := tx.GetSrc(st.Name, inKey)
	if err != nil {
		return err
	}
	if prior != nil && !bytes.Equal(prior, outKey) {
		if err := tx.DeleteIn(st.Name, prior, inKey); err != nil {
			return err
		}
		dirty.Mark(n, prior)
	}

	// Retain the input as the stage sees it (the refold working set) —
	// forEach stages retain the element WITH its parent, so drain-time
	// refolds can still reach `$parent.`.
	inputBytes, err := packStageInput(st, data, parent)
	if err != nil {
		return fmt.Errorf("stage %q: retain input: %w", st.Name, err)
	}
	if err := tx.PutIn(st.Name, outKey, inKey, encodeRetained(gen, inputBytes)); err != nil {
		return err
	}
	if err := tx.PutSrc(st.Name, inKey, outKey); err != nil {
		return err
	}
	for i := range st.Joins {
		j := &st.Joins[i]
		if onKey := joinOnKey(j, data, parent); onKey != nil {
			if err := tx.PutRev(st.Name, j.target(), onKey, outKey); err != nil {
				return err
			}
		}
	}
	dirty.Mark(n, outKey)
	return nil
}

// noteUnkeyedDelete makes a lost retraction loud: delete evidence that
// cannot key drops as non-membership like any input (payload shapes
// legitimately vary across a topic), but unlike ordinary variance it
// means a key that SHOULD die stays live — the field probe's signature:
// a deletion event captured on the topic, the liveSet key still live
// minutes later, zero dead letters. Counted for StageStat; warned once
// per stage per worker (fold path — worker goroutine only).
func (g *Graph) noteUnkeyedDelete(n *graphNode, keyPath string) {
	n.unkeyedDeletes.Add(1)
	if n.unkeyedDeleteWarned {
		return
	}
	n.unkeyedDeleteWarned = true
	zap.L().Warn("[stage] delete-shaped input could not key — its retraction is LOST (the key it meant to kill stays live)",
		zap.String("stage", n.def.Name),
		zap.String("key_path", keyPath))
}

// foldDelete retracts one input from a stage: the key it fed refolds
// without it (possibly to deletion), and the delta cascades.
func (g *Graph) foldDelete(tx *stagestore.Tx, n *graphNode, inKey []byte, dirty Dirty) error {
	if n.def.hasFan() {
		// The input's tombstone retracts every element it fanned.
		st := n.def
		var elems [][]byte
		if err := tx.DependentsOf(st.Name, fanJoinName, inKey, func(elemIn []byte) error {
			elems = append(elems, append([]byte(nil), elemIn...))
			return nil
		}); err != nil {
			return err
		}
		for _, elemIn := range elems {
			if err := g.foldDeleteInput(tx, n, elemIn, dirty); err != nil {
				return err
			}
			if err := tx.DeleteRev(st.Name, fanJoinName, inKey, elemIn); err != nil {
				return err
			}
		}
		return nil
	}
	return g.foldDeleteInput(tx, n, inKey, dirty)
}

// foldDeleteInput retracts one retained input from a stage.
func (g *Graph) foldDeleteInput(tx *stagestore.Tx, n *graphNode, inKey []byte, dirty Dirty) error {
	st := n.def
	outKey, err := tx.GetSrc(st.Name, inKey)
	if err != nil || outKey == nil {
		return err // never fed this stage — a no-op, not an error
	}
	if err := tx.DeleteIn(st.Name, outKey, inKey); err != nil {
		return err
	}
	if err := tx.DeleteSrc(st.Name, inKey); err != nil {
		return err
	}
	dirty.Mark(n, outKey)
	return nil
}

// Drain refolds every dirty key exactly once, in stage topology order
// (a stage's consumers drain after it — retention marks they receive
// here are processed later in the same pass), in sorted key order for
// determinism. A refold whose output bytes did not change is SUPPRESSED
// — no store write, no delta, no cascade — which determinism makes
// sound and which turns a batch touching one key N times into one
// refold and at most one downstream write.
func (g *Graph) Drain(tx *stagestore.Tx, dirty Dirty) error {
	for _, n := range g.order {
		keys := dirty[n]
		if len(keys) == 0 {
			continue
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			if err := g.refoldKey(tx, n, []byte(k), dirty); err != nil {
				return err
			}
		}
	}
	return nil
}

// refoldKey recomputes one key's output from its retained input set,
// stores it, and — when the bytes changed — cascades the delta (upsert
// or retraction) to sinks and consumer retention.
func (g *Graph) refoldKey(tx *stagestore.Tx, n *graphNode, outKey []byte, dirty Dirty) error {
	st := n.def
	prior, err := tx.GetOut(st.Name, outKey)
	if err != nil {
		return err
	}
	out, live, err := g.refoldOutput(tx, st, outKey)
	if err != nil {
		return err
	}
	if !live {
		if prior == nil {
			return nil // was absent, still absent — nothing to say
		}
		if err := tx.DeleteOut(st.Name, outKey); err != nil {
			return err
		}
		if g.OnDelta != nil {
			if err := g.OnDelta(st.Name, outKey, nil, false); err != nil {
				return err
			}
		}
		for _, d := range n.dimConsumers {
			if err := tx.DeleteDim(d.node.def.Name, d.join.target(), outKey); err != nil {
				return err
			}
			if err := g.markDimDependents(tx, d, outKey, dirty); err != nil {
				return err
			}
		}
		for _, c := range n.consumers {
			if err := g.foldDelete(tx, c, outKey, dirty); err != nil {
				return err
			}
		}
		for _, m := range n.mergeConsumers {
			if err := g.foldMergeSide(tx, m.node, m.alias, outKey, nil, dirty); err != nil {
				return err
			}
		}
		return nil
	}
	if prior != nil && bytes.Equal(prior, out) {
		return nil // unchanged — suppressed (deterministic bytes make this sound)
	}
	if err := tx.PutOut(st.Name, outKey, out); err != nil {
		return err
	}
	obj, err := DecodeObject(out)
	if err != nil {
		return fmt.Errorf("stage %q: decode own output: %w", st.Name, err)
	}
	if g.OnDelta != nil {
		if err := g.OnDelta(st.Name, outKey, obj, true); err != nil {
			return err
		}
	}
	for _, d := range n.dimConsumers {
		// This stage's output IS the consumer join's dimension row —
		// stage dims carry generation 0 (absorption: never swept).
		if err := tx.PutDim(d.node.def.Name, d.join.target(), outKey, encodeRetained(0, out)); err != nil {
			return err
		}
		if err := g.markDimDependents(tx, d, outKey, dirty); err != nil {
			return err
		}
	}
	for _, c := range n.consumers {
		if err := g.foldUpsert(tx, c, outKey, obj, 0, dirty); err != nil {
			return err
		}
	}
	for _, m := range n.mergeConsumers {
		if err := g.foldMergeSide(tx, m.node, m.alias, outKey, obj, dirty); err != nil {
			return err
		}
	}
	return nil
}

// foldMergeSide lands (or retracts, obj nil) one upstream's output for
// key K as that side of a merge's tuple. No when, no keyPath, no rekey:
// the merge adopts the producer's key byte-verbatim, retains the side
// under an alias-namespaced input key, and its `when` — per the unit
// rule — evaluates over the ASSEMBLED tuple at refold.
func (g *Graph) foldMergeSide(tx *stagestore.Tx, n *graphNode, alias string, key []byte, obj any, dirty Dirty) error {
	inKey := mergeInKey(alias, key)
	if obj == nil {
		if err := tx.DeleteIn(n.def.Name, key, inKey); err != nil {
			return err
		}
	} else {
		n.inputs.Add(1)
		bs, err := marshalStageObject(obj)
		if err != nil {
			return err
		}
		if err := tx.PutIn(n.def.Name, key, inKey, encodeRetained(0, bs)); err != nil {
			return err
		}
	}
	dirty.Mark(n, key)
	return nil
}

// mergeInKey namespaces a merge side's retention under its alias (the
// fan's parent/element composition trick — aliases are path-safe
// idents, so NUL cannot occur in them).
func mergeInKey(alias string, key []byte) []byte {
	out := make([]byte, 0, len(alias)+1+len(key))
	out = append(out, alias...)
	out = append(out, 0)
	return append(out, key...)
}

// refoldOutput computes a key's output object from its retained inputs.
// live=false means the key has no qualifying inputs and its output (and
// downstream memberships) retract.
func (g *Graph) refoldOutput(tx *stagestore.Tx, st *Stage, outKey []byte) (out []byte, live bool, err error) {
	if len(st.Merge) > 0 {
		return g.refoldMerge(tx, st, outKey)
	}
	if st.Reduce == "aggregate" {
		return g.refoldAggregate(tx, st, outKey)
	}
	if st.Reduce == "latest" {
		return g.refoldLatest(tx, st, outKey)
	}
	if st.Reduce == "liveSet" {
		return g.refoldLiveSet(tx, st, outKey)
	}
	// Reshape: one output object per key — deterministically the emitted
	// object of the bytewise-LARGEST input identity (real reshape inputs
	// are 1:1 with keys; the tiebreak only decides pathological
	// collisions, deterministically).
	type scoped struct{ data, parent any }
	var winner *scoped
	scanErr := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		_, payload := decodeRetained(val)
		data, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if ok, err := g.inputQualifies(tx, st, data, parent); err != nil || !ok {
			return err
		}
		winner = &scoped{data, parent}
		return nil
	})
	if scanErr != nil || winner == nil {
		return nil, false, scanErr
	}
	return g.emitReshape(tx, st, winner.data, winner.parent)
}

// joinOnKey renders an input's reference to a join's dimension row: every
// on path must resolve non-nil (a missing part = nothing to reference —
// non-participation for a normal join, vacuous absence for an anti-join),
// a single part verbatim, several through OutKey — the positional
// composite encoding both a composite-keyed stage's out keys and a
// composite topic producer's entity keys already use.
func joinOnKey(j *Join, obj, parent any) []byte {
	parts := make([]string, len(j.On))
	for i, p := range j.On {
		v, err := ResolvePath(p, obj, parent)
		if err != nil || v == nil {
			return nil
		}
		part, ok := TypedKeyPart(KeyTypeAt(j.OnType, i), v)
		if !ok {
			return nil // unrenderable reference = nothing to reference
		}
		parts[i] = NormalizeKeyPart(j.Normalize, part)
	}
	return []byte(OutKey(parts))
}

// inputQualifies applies a stage's filtering joins to one input: it
// participates only while EVERY join's dimension row — addressed by the
// input's on value — exists and matches the join's where. A dimension
// that has not arrived yet fails participation and heals when it lands
// (the fan-out refolds dependents).
// joinLookupScope overlays the input object with each NAMED join's
// current dimension row under its alias — the reference-lookup half of
// joins. The scope exists at REFOLD (emits, fold arms, per-emit where):
// dimension changes mark dependents dirty, so a pulled value re-resolves
// exactly when it can change and is never a stale retained copy. A row
// absent, or failing the join's where, scopes as nil — the LEFT JOIN's
// null side (reachable only through optional joins; a required join
// already gated membership). A non-object input passes through: it has
// no reference fields to look up with.
func (g *Graph) joinLookupScope(tx *stagestore.Tx, st *Stage, obj, parent any) (any, error) {
	named := false
	for i := range st.Joins {
		if st.Joins[i].As != "" {
			named = true
			break
		}
	}
	if !named {
		return obj, nil
	}
	m, ok := obj.(map[string]any)
	if !ok {
		return obj, nil
	}
	n := g.byName[st.Name]
	scope := make(map[string]any, len(m)+2)
	for k, v := range m {
		scope[k] = v
	}
	for i := range st.Joins {
		j := &st.Joins[i]
		if j.As == "" {
			continue
		}
		scope[j.As] = nil
		onKey := joinOnKey(j, obj, parent)
		if onKey == nil {
			continue
		}
		stored, err := tx.GetDim(st.Name, j.target(), onKey)
		if err != nil {
			return nil, err
		}
		if stored == nil {
			continue
		}
		_, payload := decodeRetained(stored)
		dimObj, err := DecodeObject(payload)
		if err != nil {
			return nil, err
		}
		if Match(j.Where, dimObj) {
			scope[j.As] = dimObj
		}
	}
	for i := range st.Joins {
		j := &st.Joins[i]
		if !j.Optional {
			continue
		}
		if scope[j.As] != nil {
			n.joinFlows[i].hits.Add(1)
		} else {
			n.joinFlows[i].misses.Add(1)
		}
	}
	return scope, nil
}

func (g *Graph) inputQualifies(tx *stagestore.Tx, st *Stage, obj, parent any) (bool, error) {
	n := g.byName[st.Name]
	for i := range st.Joins {
		j := &st.Joins[i]
		if j.Optional {
			continue // a LEFT lookup never gates membership; absence scopes as null (counted at lookup)
		}
		// present: a dimension row exists at the input's On value AND
		// matches the join's Where. A normal join requires it; an Absent
		// (anti-)join forbids it — one rule: fail when present == Absent.
		present := false
		if onKey := joinOnKey(j, obj, parent); onKey != nil {
			stored, err := tx.GetDim(st.Name, j.target(), onKey)
			if err != nil {
				return false, err
			}
			if stored != nil {
				_, payload := decodeRetained(stored)
				dimObj, err := DecodeObject(payload)
				if err != nil {
					return false, err
				}
				present = Match(j.Where, dimObj)
			}
		}
		if present {
			n.joinFlows[i].hits.Add(1)
		} else {
			n.joinFlows[i].misses.Add(1)
		}
		if present == j.Absent {
			return false, nil
		}
	}
	return true, nil
}

// refoldMerge assembles a merge key's tuple — each retained side's
// object under its alias — gates it through the stage's when (the unit
// rule: the tuple IS the fold unit, which is what lets notNull test
// sibling presence), and emits. No sides retained = the key retracts.
func (g *Graph) refoldMerge(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	// The tuple carries EVERY declared side — absent ones as explicit
	// nulls, exactly as an outer join produces NULL columns — so the
	// null/notNull arms address absence naturally (a missing map entry
	// would never match `null = true`, which requires a PRESENT null).
	scope := make(map[string]any, len(st.Merge))
	for _, e := range st.Merge {
		scope[e.As] = nil
	}
	present := 0
	err := tx.InputsFor(st.Name, outKey, func(inKey, val []byte) error {
		i := bytes.IndexByte(inKey, 0)
		if i < 0 {
			return nil
		}
		_, payload := decodeRetained(val)
		obj, derr := DecodeObject(payload)
		if derr != nil {
			return fmt.Errorf("merge %q: decode side %q: %w", st.Name, string(inKey[:i]), derr)
		}
		scope[string(inKey[:i])] = obj
		present++
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if present == 0 {
		return nil, false, nil
	}
	if !Match(st.When, scope) {
		return nil, false, nil
	}
	return g.emitReshape(tx, st, scope, nil)
}

// emitReshape renders a stage's emit fields from one input object — the
// reshape path, and the winner path of reduce = "latest" — over the
// join-lookup scope (named joins' rows under their aliases).
//
// Emit-evaluation failures are DATA errors — deterministic for their
// input (the same entity fails identically on every replay) — so they
// wrap cluster.Permanent and the worker DEAD-LETTERS the triggering
// Actual, exactly as the expression language documents. The rollback
// makes this replay-consistent: the offending input is never retained,
// so later refolds of the key succeed without it (the same argument
// entity-decode failures already rely on). Store/retention integrity
// errors stay UNWRAPPED: those must wedge loudly, not skip.
func (g *Graph) emitReshape(tx *stagestore.Tx, st *Stage, obj, parent any) ([]byte, bool, error) {
	obj, err := g.joinLookupScope(tx, st, obj, parent)
	if err != nil {
		return nil, false, err
	}
	if st.identityEmit {
		// A synthesized identity stage re-emits its input verbatim
		// (re-marshaled, so keys sort canonically like every output).
		bs, err := marshalStageObject(obj)
		return bs, true, err
	}
	emitted := make(map[string]any, len(st.Emit))
	for i := range st.Emit {
		e := &st.Emit[i]
		var v any
		if e.From != "" {
			if v, err = ResolvePath(e.From, obj, parent); err != nil {
				v = nil // a missing field is null, per the language
			}
		} else {
			if v, err = Eval(e.compiled, obj, parent); err != nil {
				return nil, false, cluster.Permanent(fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err))
			}
		}
		emitted[e.Field], err = stageValue(v)
		if err != nil {
			return nil, false, cluster.Permanent(fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err))
		}
	}
	bs, err := marshalStageObject(emitted)
	return bs, true, err
}

// refoldLiveSet recomputes a created-minus-deleted key: LIVE iff at
// least one qualifying non-delete input and ZERO delete-shaped inputs —
// a set difference, so no ordering is needed and retracting the delete
// event itself un-deletes the key. The live key emits from its
// bytewise-largest non-delete input, like a reshape.
func (g *Graph) refoldLiveSet(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	type scoped struct{ data, parent any }
	var winner *scoped
	dead := false
	err := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		if dead {
			return nil
		}
		_, payload := decodeRetained(val)
		data, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if Match(st.DeleteWhen, data) {
			dead = true
			winner = nil
			return nil
		}
		if ok, err := g.inputQualifies(tx, st, data, parent); err != nil || !ok {
			return err
		}
		winner = &scoped{data, parent}
		return nil
	})
	if err != nil || dead || winner == nil {
		return nil, false, err
	}
	return g.emitReshape(tx, st, winner.data, winner.parent)
}

// refoldLatest recomputes an argmax key: the winner is the retained input
// with the greatest OrderBy value — a BUSINESS field, never arrival order,
// so backfills in keyset order and steady-state in log order converge —
// with TieBy the mandatory deterministic tiebreak. The stage's `when`
// filtered inputs before they were retained, so the argmax runs over the
// qualifying set only (an unapproved newer input never shadows an
// approved older one). The winner's object then emits like a reshape.
func (g *Graph) refoldLatest(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	type ranked struct {
		order, tie  any
		obj, parent any
	}
	var winner *ranked
	err := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		_, payload := decodeRetained(val)
		obj, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if ok, err := g.inputQualifies(tx, st, obj, parent); err != nil || !ok {
			return err
		}
		ov, _ := ResolvePath(st.OrderBy, obj, parent)
		tv, _ := ResolvePath(st.TieBy, obj, parent)
		cand := &ranked{order: ov, tie: tv, obj: obj, parent: parent}
		if winner == nil {
			winner = cand
			return nil
		}
		if c := compareStageValues(cand.order, winner.order, st.OrderByType == KeyTypeNumber); c > 0 {
			winner = cand
		} else if c == 0 {
			if compareStageValues(cand.tie, winner.tie, st.TieByType == KeyTypeNumber) > 0 {
				winner = cand
			}
		}
		return nil
	})
	if err != nil || winner == nil {
		return nil, false, err
	}
	return g.emitReshape(tx, st, winner.obj, winner.parent)
}

// compareStageValues orders two payload values for argmax: nulls sort
// lowest (a missing business timestamp never wins), then numeric or
// lexical per the declared type. In the numeric mode an unparsable value
// ranks with the nulls — below every real number, deterministically.
func compareStageValues(a, b any, numeric bool) int {
	ar, aOK := stageComparable(a, numeric)
	br, bOK := stageComparable(b, numeric)
	if !aOK || !bOK {
		switch {
		case aOK:
			return 1
		case bOK:
			return -1
		default:
			return 0
		}
	}
	if numeric {
		return ar.(*big.Rat).Cmp(br.(*big.Rat))
	}
	as, bs := ar.(string), br.(string)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

func stageComparable(v any, numeric bool) (any, bool) {
	if v == nil {
		return nil, false
	}
	if numeric {
		switch t := v.(type) {
		case json.Number:
			if r, ok := new(big.Rat).SetString(string(t)); ok {
				return r, true
			}
		case string:
			if r, ok := new(big.Rat).SetString(t); ok {
				return r, true
			}
		}
		return nil, false
	}
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return string(t), true
	case bool:
		if t {
			return "true", true
		}
		return "false", true
	}
	return nil, false
}

// refoldAggregate recomputes an aggregate key: every fold arm evaluates
// over the FULL retained input set (recompute, never increment). SQL
// empty-set semantics: no inputs → the key retracts entirely; a null arm
// operand skips that input for that arm (sum/min/max of nothing is null,
// count counts rows).
func (g *Graph) refoldAggregate(tx *stagestore.Tx, st *Stage, outKey []byte) ([]byte, bool, error) {
	sums := make([]*big.Rat, len(st.Emit))
	mins := make([]any, len(st.Emit))
	maxs := make([]any, len(st.Emit))
	counts := make([]int, len(st.Emit))
	collects := make([][]any, len(st.Emit))
	count := 0

	err := tx.InputsFor(st.Name, outKey, func(_, val []byte) error {
		_, payload := decodeRetained(val)
		obj, parent, err := unpackStageInput(st, payload)
		if err != nil {
			return err
		}
		if ok, err := g.inputQualifies(tx, st, obj, parent); err != nil || !ok {
			return err
		}
		count++
		// Fold arms and per-emit wheres see the join-lookup scope: an
		// aggregate can sum a looked-up row's field (SQL: aggregating a
		// joined column).
		obj, err = g.joinLookupScope(tx, st, obj, parent)
		if err != nil {
			return err
		}
		for i := range st.Emit {
			e := &st.Emit[i]
			// Per-emit where (SQL's FILTER): this input skips THIS
			// field's fold only — membership already counted above.
			if len(e.Where) > 0 && !MatchScoped(e.Where, obj, parent) {
				continue
			}
			if e.Count {
				counts[i]++
				continue
			}
			v, err := Eval(e.compiled, obj, parent)
			if err != nil {
				return cluster.Permanent(fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err))
			}
			if e.Collect != "" {
				if v != nil {
					collects[i] = append(collects[i], v)
				}
				continue
			}
			// Min/max order ANY scalar — numbers numerically, text
			// lexically (SQL's MIN/MAX over dates-as-strings, the field
			// case a numeric-only gate silently nulled), bools false<true
			// — by collect's total order. Sum stays numeric (SQL agrees);
			// nulls skip everywhere.
			switch {
			case e.Min != "":
				if v != nil && (mins[i] == nil || collectLess(v, mins[i])) {
					mins[i] = v
				}
				continue
			case e.Max != "":
				if v != nil && (maxs[i] == nil || collectLess(maxs[i], v)) {
					maxs[i] = v
				}
				continue
			}
			r, ok := v.(*big.Rat)
			if !ok {
				// A null skips a sum (SQL); a non-numeric passthrough also
				// skips — OUR filter posture, where SQL would error the
				// query (sum over text has no better answer here than
				// null-like absence).
				continue
			}
			if e.Sum != "" {
				if sums[i] == nil {
					sums[i] = new(big.Rat)
				}
				sums[i].Add(sums[i], r)
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if count == 0 {
		return nil, false, nil
	}

	emitted := make(map[string]any, len(st.Emit))
	for i := range st.Emit {
		e := &st.Emit[i]
		var v any
		switch {
		case e.Count:
			v = json.Number(fmt.Sprintf("%d", counts[i]))
		case e.Collect != "":
			vals := collects[i]
			if e.Distinct {
				vals = distinctCollectValues(vals)
			}
			sortCollectValues(vals)
			arr := make([]any, len(vals))
			for k, cv := range vals {
				if arr[k], err = stageValue(cv); err != nil {
					return nil, false, cluster.Permanent(fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err))
				}
			}
			v = arr
		case e.Sum != "":
			if sums[i] != nil {
				v = sums[i]
			}
		case e.Min != "":
			if mins[i] != nil {
				v = mins[i]
			}
		case e.Max != "":
			if maxs[i] != nil {
				v = maxs[i]
			}
		}
		emitted[e.Field], err = stageValue(v)
		if err != nil {
			return nil, false, cluster.Permanent(fmt.Errorf("stage %q emit %q: %w", st.Name, e.Field, err))
		}
	}
	bs, err := marshalStageObject(emitted)
	return bs, true, err
}

// stageValue renders an evaluator value into its stored JSON form:
// rationals become exact minimal-decimal json.Numbers, scalars pass
// through, null stays null.
func stageValue(v any) (any, error) {
	if r, ok := v.(*big.Rat); ok {
		text, err := FormatRat(r)
		if err != nil {
			return nil, err
		}
		return json.Number(text), nil
	}
	return v, nil
}

// sortCollectValues orders evaluator scalars deterministically: numbers
// (numerically) before strings (lexically) before bools (false, true) —
// a total order, so equal folds are byte-equal. The deliberate
// divergence from SQL: array_agg without ORDER BY is nondeterministic,
// and the store's self-healing arguments lean on byte-equal refolds.
func sortCollectValues(vals []any) {
	sort.SliceStable(vals, func(a, b int) bool { return collectLess(vals[a], vals[b]) })
}

func collectLess(a, b any) bool {
	fa, fb := collectFamily(a), collectFamily(b)
	if fa != fb {
		return fa < fb
	}
	switch fa {
	case 0:
		return a.(*big.Rat).Cmp(b.(*big.Rat)) < 0
	case 1:
		return a.(string) < b.(string)
	default:
		return !a.(bool) && b.(bool)
	}
}

func collectFamily(v any) int {
	switch v.(type) {
	case *big.Rat:
		return 0
	case string:
		return 1
	default:
		return 2
	}
}

// distinctCollectValues dedupes by typed value (the family tag keeps
// the number 5 and the string "5" distinct), preserving first
// appearance — order is irrelevant, the sort follows.
func distinctCollectValues(vals []any) []any {
	seen := make(map[string]bool, len(vals))
	out := vals[:0]
	for _, v := range vals {
		var key string
		switch t := v.(type) {
		case *big.Rat:
			key = "n|" + t.RatString()
		case string:
			key = "s|" + t
		case bool:
			key = fmt.Sprintf("b|%t", t)
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, v)
	}
	return out
}

// fanJoinName is the reserved reverse-index name a forEach stage uses to
// enumerate the element-inputs each parent currently fans (its
// reconciliation set). The NUL prefix keeps it out of any real join's
// topic namespace.
const fanJoinName = "\x00fan"

// fanInKey is a fanned element-input's identity: (parent input key,
// element out-key rendering), length-framed so any parent key bytes scan
// correctly.
// fanArmElemKey namespaces an element identity by its fan arm — a NUL
// lead (the reserved-namespace convention, see fanJoinName) keeps
// arm-scoped identities disjoint from single-path forEach's raw element
// keys, and the uvarint ordinal keeps arms disjoint from each other, so
// equal element keys via different arms are distinct inputs (UNION ALL).
func fanArmElemKey(ordinal int, elemKey []byte) []byte {
	var lead [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lead[:], uint64(max(ordinal, 0))) // ordinal is an arm index, never negative
	out := make([]byte, 0, 1+n+len(elemKey))
	out = append(out, 0x00)
	out = append(out, lead[:n]...)
	return append(out, elemKey...)
}

func fanInKey(parentKey, elemKey []byte) []byte {
	var lead [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lead[:], uint64(len(parentKey)))
	out := make([]byte, 0, n+len(parentKey)+len(elemKey))
	out = append(out, lead[:n]...)
	out = append(out, parentKey...)
	return append(out, elemKey...)
}

// packStageInput renders the retained object for one input of a stage.
// forEach stages retain a WRAPPER per element — the element plus its
// enclosing input under fixed keys "e"/"p" — because refolds happen at
// drain time, after the enclosing event is gone, and emits/joins may
// reach `$parent.`.
func packStageInput(st *Stage, data, parent any) ([]byte, error) {
	if !st.hasFan() {
		return marshalStageObject(data)
	}
	return marshalStageObject(map[string]any{"e": data, "p": parent})
}

// unpackStageInput decodes a retained payload back into (element scope,
// parent scope) for refold-time path resolution.
func unpackStageInput(st *Stage, payload []byte) (data, parent any, err error) {
	obj, err := DecodeObject(payload)
	if err != nil {
		return nil, nil, err
	}
	if !st.hasFan() {
		return obj, nil, nil
	}
	m, ok := obj.(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("forEach retention is not a wrapper object")
	}
	return m["e"], m["p"], nil
}

// encodeRetained prefixes a retained value with its 8-byte BE
// generation — the refresh epoch the input was captured under. Stage-fed
// retention and direct user writes carry generation 0, which no sweep
// ever removes (the sink sweep's >= 1 floor, mirrored: upstream
// refreshes reach stage-fed retention as EXPLICIT deltas, never sweeps —
// no epoch transits a stateful stage).
func encodeRetained(gen uint64, payload []byte) []byte {
	out := make([]byte, 8, 8+len(payload))
	binary.BigEndian.PutUint64(out, gen)
	return append(out, payload...)
}

// decodeRetained splits a retained value into generation and payload.
func decodeRetained(val []byte) (uint64, []byte) {
	if len(val) < 8 {
		return 0, val // pre-encoding store state: rebuilt anyway (format/fingerprint)
	}
	return binary.BigEndian.Uint64(val[:8]), val[8:]
}

// marshalStageObject renders a stage object deterministically: Go maps
// marshal with sorted keys, and json.Number values keep their exact
// digits — equal folds are byte-equal.
func marshalStageObject(obj any) ([]byte, error) { return json.Marshal(obj) }

// DecodeObject decodes with UseNumber so exact digits survive the
// store round-trip.
func DecodeObject(bs []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(bs))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// OutKey renders a stage key: a single part verbatim, several parts
// through the producers' composite encoding (cluster.CompositeKey with
// synthetic column names), so a stage-fed table's composite tombstone
// machinery decodes it unchanged.
func OutKey(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	m := make(map[string]any, len(parts))
	cols := make([]string, len(parts))
	for i, part := range parts {
		c := fmt.Sprintf("k%d", i)
		cols[i] = c
		m[c] = part
	}
	return cluster.CompositeKey(m, cols)
}

// coerceKeyScalar renders a key path value into the canonical key space
// (numbers keep source digits; strings pass through).
func coerceKeyScalar(v any) any { return v }
