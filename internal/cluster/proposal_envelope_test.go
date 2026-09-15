package cluster

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// These tests pin the typed control envelope (clusterpb.LogEntity.body):
// writers encode every entity as exactly one body variant, and readers map it
// into one logical view through the logEntityView chokepoint. Pre-envelope
// flat bytes (<= 0.7.2-beta, below the 0.8.0 data-dir floor of 0.7.3-beta)
// must fail decode loudly — pinned here with hand-built protowire bytes.

func envelopeTestResolver(tp *Type) *stubResolver {
	return &stubResolver{
		types:    map[string]*Type{tp.ID: tp},
		versions: map[string]*Type{fmt.Sprintf("%s@%d", tp.ID, tp.Version): tp},
	}
}

// entityFixtures returns one Entity per envelope variant, exercising every
// field each variant carries: a generation-stamped row, a generation-stamped
// keep-data delete, and a refresh-boundary marker.
func entityFixtures(tp *Type) []*Entity {
	row := NewUpsertEntity(tp, []byte("row-key"), []byte("row-data"))
	row.Generation = 3
	del := NewDeleteEntity(tp, []byte("del-key"))
	del.Generation = 4
	del.KeepData = true
	refresh := NewRefreshBoundaryEntity(tp, 5)
	return []*Entity{row, del, refresh}
}

func assertFixtureEntities(t *testing.T, es []*Entity) {
	t.Helper()
	if len(es) != 3 {
		t.Fatalf("got %d entities, want 3", len(es))
	}
	row, del, refresh := es[0], es[1], es[2]
	if string(row.Key) != "row-key" || string(row.Data) != "row-data" || row.Generation != 3 ||
		row.IsDelete() || row.IsRefreshBoundary() || row.KeepData {
		t.Errorf("row decoded wrong: %+v", row)
	}
	if string(del.Key) != "del-key" || !del.IsDelete() || del.Generation != 4 ||
		!del.KeepData || del.IsRefreshBoundary() {
		t.Errorf("delete decoded wrong: %+v", del)
	}
	if !refresh.IsRefreshBoundary() || refresh.Generation != 5 ||
		refresh.IsDelete() || refresh.Key != nil || refresh.KeepData {
		t.Errorf("refresh decoded wrong: %+v", refresh)
	}
}

// TestEnvelopeRoundTrip proves each variant survives Marshal → Unmarshal with
// every field intact.
func TestEnvelopeRoundTrip(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	p := &Proposal{Entities: entityFixtures(tp)}
	bs, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	dec := &Proposal{}
	if err := dec.Unmarshal(bs, envelopeTestResolver(tp)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	assertFixtureEntities(t, dec.Entities)
}

// TestEnvelopeWritersEmitVariantsOnly raw-decodes a marshaled proposal and
// asserts the wire shape directly: every entity carries exactly the matching
// body variant and none of the legacy flat fields. This is the writer-side pin
// — if Marshal ever regresses to the flat encoding (or double-writes), this
// fails even though the round-trip test would still pass.
func TestEnvelopeWritersEmitVariantsOnly(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	bs, err := (&Proposal{Entities: entityFixtures(tp)}).Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	lp := &clusterpb.LogProposal{}
	if err := proto.Unmarshal(bs, lp); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if len(lp.LogEntities) != 3 {
		t.Fatalf("got %d wire entities, want 3", len(lp.LogEntities))
	}
	// (No flat-field check: the legacy flat fields were removed from the
	// schema when the floor rose to 0.7.3-beta, so the type system enforces
	// what this loop used to assert.)
	row := lp.LogEntities[0].GetRow()
	if row == nil || string(row.GetKey()) != "row-key" || string(row.GetData()) != "row-data" || row.GetGeneration() != 3 {
		t.Errorf("entity 0 is not the expected Row variant: %+v", lp.LogEntities[0].GetBody())
	}
	del := lp.LogEntities[1].GetDelete()
	if del == nil || string(del.GetKey()) != "del-key" || del.GetGeneration() != 4 || !del.GetKeepData() {
		t.Errorf("entity 1 is not the expected Delete variant: %+v", lp.LogEntities[1].GetBody())
	}
	if d := del.GetKey(); string(d) == string(delete) {
		t.Errorf("Delete variant carries the in-memory sentinel on the wire")
	}
	refresh := lp.LogEntities[2].GetRefresh()
	if refresh == nil || refresh.GetGeneration() != 5 {
		t.Errorf("entity 2 is not the expected Refresh variant: %+v", lp.LogEntities[2].GetBody())
	}
}

// legacyFlatProposal hand-builds the pre-envelope wire form at the raw
// protowire level — flat Key (tag 2) / Data (tag 3, the delete sentinel for
// the delete), generation (tag 5), refresh_boundary (tag 6) — exactly what a
// <= 0.7.2-beta binary wrote. The tags are reserved in the schema now (the
// data-dir floor is 0.7.3-beta), so the generated types can no longer express
// this shape and the bytes must be built by hand.
func legacyFlatProposal(t *testing.T, tp *Type) []byte {
	t.Helper()
	ref, err := proto.Marshal(&clusterpb.TypeRef{ID: tp.ID, Version: uint32(tp.Version)})
	if err != nil {
		t.Fatalf("proto.Marshal TypeRef: %v", err)
	}
	entity := func(build func([]byte) []byte) []byte {
		var e []byte
		e = protowire.AppendTag(e, 1, protowire.BytesType) // type
		e = protowire.AppendBytes(e, ref)
		return build(e)
	}
	flatRow := entity(func(e []byte) []byte {
		e = protowire.AppendTag(e, 2, protowire.BytesType) // Key
		e = protowire.AppendBytes(e, []byte("row-key"))
		e = protowire.AppendTag(e, 3, protowire.BytesType) // Data
		e = protowire.AppendBytes(e, []byte("row-data"))
		e = protowire.AppendTag(e, 5, protowire.VarintType) // generation
		e = protowire.AppendVarint(e, 3)
		return e
	})
	flatDelete := entity(func(e []byte) []byte {
		e = protowire.AppendTag(e, 2, protowire.BytesType)
		e = protowire.AppendBytes(e, []byte("del-key"))
		e = protowire.AppendTag(e, 3, protowire.BytesType)
		e = protowire.AppendBytes(e, delete)
		e = protowire.AppendTag(e, 5, protowire.VarintType)
		e = protowire.AppendVarint(e, 4)
		return e
	})
	flatMarker := entity(func(e []byte) []byte {
		e = protowire.AppendTag(e, 5, protowire.VarintType)
		e = protowire.AppendVarint(e, 5)
		e = protowire.AppendTag(e, 6, protowire.VarintType) // refresh_boundary
		e = protowire.AppendVarint(e, 1)
		return e
	})
	var lp []byte
	for _, e := range [][]byte{flatRow, flatDelete, flatMarker} {
		lp = protowire.AppendTag(lp, 1, protowire.BytesType) // logEntities
		lp = protowire.AppendBytes(lp, e)
	}
	return lp
}

// TestLegacyFlatEntitiesFailLoudly pins the 0.8.0 data-dir floor (0.7.3-beta):
// pre-envelope flat bytes — written only by <= 0.7.2-beta, an era no
// deployment ever ran — must FAIL decode with the floor error, never silently
// apply as empty entities (their reserved tags land in the unknown-fields
// guard at the logEntityView chokepoint).
func TestLegacyFlatEntitiesFailLoudly(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	dec := &Proposal{}
	err := dec.Unmarshal(legacyFlatProposal(t, tp), envelopeTestResolver(tp))
	if err == nil {
		t.Fatal("pre-envelope flat bytes decoded without error; the 0.7.3-beta floor must reject them loudly")
	}
	if !strings.Contains(err.Error(), "0.7.3-beta") {
		t.Errorf("floor error should name the supported floor, got: %v", err)
	}
}

// TestScrubTraversalsHandleEnvelope pins the scrub-side wire readers:
// FilterProposalEntities and ForEachProposalEntity see the (key, isDelete)
// view of every envelope record — and fail LOUDLY on pre-floor flat bytes
// (the sibling test below), never silently dropping entities from a rewrite.
func TestScrubTraversalsHandleEnvelope(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	envelope, err := (&Proposal{Entities: entityFixtures(tp)}).Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for name, raw := range map[string][]byte{
		"envelope": envelope,
	} {
		t.Run(name, func(t *testing.T) {
			// ForEachProposalEntity: collect the traversal's view.
			type seen struct {
				key      string
				isDelete bool
			}
			var got []seen
			err := ForEachProposalEntity(raw, func(typeID string, key, data []byte, isDelete bool) error {
				if typeID != tp.ID {
					t.Errorf("typeID = %q, want %q", typeID, tp.ID)
				}
				if isDelete != (string(data) == string(delete)) {
					t.Errorf("key %q: isDelete=%v disagrees with data", key, isDelete)
				}
				got = append(got, seen{key: string(key), isDelete: isDelete})
				return nil
			})
			if err != nil {
				t.Fatalf("ForEachProposalEntity: %v", err)
			}
			want := []seen{{"row-key", false}, {"del-key", true}, {"", false}}
			if len(got) != len(want) {
				t.Fatalf("saw %d entities, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("entity %d: got %+v, want %+v", i, got[i], want[i])
				}
			}

			// FilterProposalEntities: remove the delete tombstone by key, keep
			// the rest, and confirm the rewrite preserves the survivors.
			out, allRemoved, changed, err := FilterProposalEntities(raw, func(typeID string, key []byte, isDelete bool) bool {
				return isDelete && string(key) == "del-key"
			})
			if err != nil {
				t.Fatalf("FilterProposalEntities: %v", err)
			}
			if allRemoved || !changed {
				t.Fatalf("allRemoved=%v changed=%v, want false/true", allRemoved, changed)
			}
			kept := decodeEntities(t, out)
			if len(kept) != 2 || kept[0][1] != "row-key" || kept[1][1] != "" {
				t.Errorf("kept entities = %v, want [row-key, refresh-marker]", kept)
			}
		})
	}
}

// TestScrubTraversalsRejectLegacyFlat: the scrubber's deterministic rewrite
// must never run over bytes it cannot fully read — pre-floor flat records
// error out of both traversals instead of being treated as empty (which would
// silently drop entities from the rewritten log).
func TestScrubTraversalsRejectLegacyFlat(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	raw := legacyFlatProposal(t, tp)
	if err := ForEachProposalEntity(raw, func(string, []byte, []byte, bool) error { return nil }); err == nil {
		t.Error("ForEachProposalEntity accepted pre-floor flat bytes")
	}
	if _, _, _, err := FilterProposalEntities(raw, func(string, []byte, bool) bool { return false }); err == nil {
		t.Error("FilterProposalEntities accepted pre-floor flat bytes")
	}
}

// TestEntityVariantClassification pins Variant(): each constructor maps to its
// variant, and the precedence (refresh > delete > row) is centralized — an
// impossible in-memory combination (marker flag AND sentinel Data) resolves
// the same way every consumer switch will see it.
func TestEntityVariantClassification(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	if v := NewUpsertEntity(tp, []byte("k"), []byte("d")).Variant(); v != EntityVariantRow {
		t.Errorf("upsert classifies as %v, want row", v)
	}
	if v := NewDeleteEntity(tp, []byte("k")).Variant(); v != EntityVariantDelete {
		t.Errorf("delete classifies as %v, want delete", v)
	}
	if v := NewRefreshBoundaryEntity(tp, 2).Variant(); v != EntityVariantRefresh {
		t.Errorf("refresh marker classifies as %v, want refresh", v)
	}
	// The impossible combination: marker flag set AND sentinel Data. The
	// precedence must resolve it as refresh, mirroring the old mandatory
	// check-IsRefreshBoundary-first ritual.
	impossible := NewDeleteEntity(tp, []byte("k"))
	impossible.RefreshBoundary = true
	if v := impossible.Variant(); v != EntityVariantRefresh {
		t.Errorf("marker+sentinel classifies as %v, want refresh (precedence)", v)
	}
}

// unknownFieldEntity returns a marshaled proposal whose single entity carries
// a LogEntity-level wire tag this binary does not know — what a future
// release's new body variant looks like to us (field 10, bytes wire type).
func unknownFieldEntity(t *testing.T, tp *Type) []byte {
	t.Helper()
	le := &clusterpb.LogEntity{
		Type: &clusterpb.TypeRef{ID: tp.ID, Version: uint32(tp.Version)},
	}
	// field 10, wire type 2 (bytes): tag byte (10<<3)|2 = 0x52, length 3.
	le.ProtoReflect().SetUnknown([]byte{0x52, 0x03, 'f', 'u', 't'})
	bs, err := proto.Marshal(&clusterpb.LogProposal{LogEntities: []*clusterpb.LogEntity{le}})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	return bs
}

// TestUnknownBodyVariantIsALoudDecodeError is the defense-in-depth behind the
// feature gate: an entity carrying LogEntity-level wire tags this binary does
// not know (a body variant from a newer release) must FAIL decode — in
// Unmarshal and in both scrub traversals — not fall into the legacy flat path
// and silently apply as an empty entity.
func TestUnknownBodyVariantIsALoudDecodeError(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	raw := unknownFieldEntity(t, tp)

	if err := (&Proposal{}).Unmarshal(raw, envelopeTestResolver(tp)); err == nil {
		t.Error("Unmarshal accepted an unknown body variant; want a loud error")
	}
	if _, _, _, err := FilterProposalEntities(raw, func(string, []byte, bool) bool { return false }); err == nil {
		t.Error("FilterProposalEntities accepted an unknown body variant; want a loud error")
	}
	err := ForEachProposalEntity(raw, func(string, []byte, []byte, bool) error { return nil })
	if err == nil {
		t.Error("ForEachProposalEntity accepted an unknown body variant; want a loud error")
	}
}

// TestUnknownFieldInsideVariantStaysAddOnly pins the boundary of the guard:
// unknown tags INSIDE a known variant's message (a future release adding a
// field to LogRow) are ordinary add-only protobuf evolution and must decode
// fine — only LogEntity-level unknowns are a variant this binary can't apply.
func TestUnknownFieldInsideVariantStaysAddOnly(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	row := &clusterpb.LogRow{Key: []byte("k"), Data: []byte("d")}
	// field 4, wire type 2: tag byte (4<<3)|2 = 0x22, length 2.
	row.ProtoReflect().SetUnknown([]byte{0x22, 0x02, 'h', 'i'})
	bs, err := proto.Marshal(&clusterpb.LogProposal{LogEntities: []*clusterpb.LogEntity{{
		Type: &clusterpb.TypeRef{ID: tp.ID, Version: uint32(tp.Version)},
		Body: &clusterpb.LogEntity_Row{Row: row},
	}}})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	dec := &Proposal{}
	if err := dec.Unmarshal(bs, envelopeTestResolver(tp)); err != nil {
		t.Fatalf("a new field inside LogRow must stay add-only compatible, got error: %v", err)
	}
	if len(dec.Entities) != 1 || string(dec.Entities[0].Key) != "k" || string(dec.Entities[0].Data) != "d" {
		t.Errorf("row with an extra unknown field decoded wrong: %+v", dec.Entities)
	}
}
