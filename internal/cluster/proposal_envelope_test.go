package cluster

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// These tests pin the typed control envelope (clusterpb.LogEntity.body):
// writers encode every entity as exactly one body variant, readers map either
// encoding — envelope or the legacy flat fields written by <= 0.7.2-beta —
// into the same logical view through the logEntityView chokepoint.

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
	for i, le := range lp.LogEntities {
		if le.Key != nil || le.Data != nil || le.GetGeneration() != 0 || le.GetRefreshBoundary() {
			t.Errorf("entity %d sets legacy flat fields: %+v", i, le)
		}
	}
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

// legacyFlatProposal hand-builds the pre-envelope wire form of entityFixtures:
// flat Key/Data (the delete sentinel for the delete), generation, and
// refresh_boundary — exactly what a <= 0.7.2-beta binary wrote. keepData has
// no legacy form; it shipped with the envelope.
func legacyFlatProposal(t *testing.T, tp *Type) []byte {
	t.Helper()
	ref := &clusterpb.TypeRef{ID: tp.ID, Version: uint32(tp.Version)}
	bs, err := proto.Marshal(&clusterpb.LogProposal{
		LogEntities: []*clusterpb.LogEntity{
			{Type: ref, Key: []byte("row-key"), Data: []byte("row-data"), Generation: 3},
			{Type: ref, Key: []byte("del-key"), Data: delete, Generation: 4},
			{Type: ref, Generation: 5, RefreshBoundary: true},
		},
	})
	if err != nil {
		t.Fatalf("proto.Marshal legacy: %v", err)
	}
	return bs
}

// TestEnvelopeDecodesLegacyFlatEntities is the reader-side back-compat pin: a
// log written by <= 0.7.2-beta (flat fields, no body variant) must decode to
// the same Entities as the envelope encoding — except KeepData, which has no
// legacy form. Deleting the legacy branch from logEntityView fails this.
func TestEnvelopeDecodesLegacyFlatEntities(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	dec := &Proposal{}
	if err := dec.Unmarshal(legacyFlatProposal(t, tp), envelopeTestResolver(tp)); err != nil {
		t.Fatalf("Unmarshal legacy bytes: %v", err)
	}
	if len(dec.Entities) != 3 {
		t.Fatalf("got %d entities, want 3", len(dec.Entities))
	}
	// The legacy delete can't carry KeepData; align it so the shared assertion
	// checks everything else.
	if dec.Entities[1].KeepData {
		t.Errorf("legacy delete decoded KeepData=true; keepData has no legacy form")
	}
	dec.Entities[1].KeepData = true
	assertFixtureEntities(t, dec.Entities)
}

// TestScrubTraversalsHandleBothEncodings pins the scrub-side wire readers:
// FilterProposalEntities and ForEachProposalEntity must see the same
// (key, isDelete) view whether the record is envelope-encoded or legacy flat —
// a scrub of a mixed-age log walks both.
func TestScrubTraversalsHandleBothEncodings(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}
	envelope, err := (&Proposal{Entities: entityFixtures(tp)}).Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for name, raw := range map[string][]byte{
		"envelope": envelope,
		"legacy":   legacyFlatProposal(t, tp),
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
