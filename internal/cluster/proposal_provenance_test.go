package cluster

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// TestProvenanceRoundTrip proves the capture-provenance pair (source commit
// time + source transaction id) survives Marshal/Unmarshal alongside the
// other proposal-level ingest fields.
func TestProvenanceRoundTrip(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}

	p := &Proposal{
		Entities:             []*Entity{NewUpsertEntity(tp, []byte("k"), []byte("d"))},
		IngestableID:         "ing-1",
		SourceSeq:            42,
		SourceCommitUnixNano: 1_755_000_000_000_000_000,
		SourceTxnID:          "3e11fa47-71ca-11e1-9e33-c80aa9429562:23",
	}
	bs, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	resolver := &stubResolver{
		types:    map[string]*Type{tp.ID: tp},
		versions: map[string]*Type{fmt.Sprintf("%s@%d", tp.ID, tp.Version): tp},
	}
	got := &Proposal{}
	if err := got.Unmarshal(bs, resolver); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.SourceCommitUnixNano != p.SourceCommitUnixNano {
		t.Errorf("SourceCommitUnixNano = %d, want %d", got.SourceCommitUnixNano, p.SourceCommitUnixNano)
	}
	if got.SourceTxnID != p.SourceTxnID {
		t.Errorf("SourceTxnID = %q, want %q", got.SourceTxnID, p.SourceTxnID)
	}
}

// TestZeroProvenanceWireBackCompatible pins the absent-provenance contract:
// an unstamped proposal (snapshot phase, direct user write) marshals
// byte-identically to one built before the fields existed — proto3 omits
// zero-valued scalars — and pre-feature log bytes unmarshal to the zero
// values that mean "not captured".
func TestZeroProvenanceWireBackCompatible(t *testing.T) {
	tp := &Type{ID: "topic-id", Name: "Topic", Version: 1}

	p := &Proposal{Entities: []*Entity{NewUpsertEntity(tp, []byte("k"), []byte("d"))}}
	got, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// The pre-feature wire form: a LogProposal that never mentions the
	// provenance fields.
	want, err := proto.Marshal(&clusterpb.LogProposal{
		LogEntities: []*clusterpb.LogEntity{{
			Type: &clusterpb.TypeRef{ID: tp.ID, Version: uint32(tp.Version)},
			Body: &clusterpb.LogEntity_Row{Row: &clusterpb.LogRow{
				Key:  []byte("k"),
				Data: []byte("d"),
			}},
		}},
	})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("zero-provenance proposal is not wire-identical to a pre-feature proposal:\n got  %x\n want %x", got, want)
	}

	// And those pre-feature bytes decode to the zero values.
	resolver := &stubResolver{
		types:    map[string]*Type{tp.ID: tp},
		versions: map[string]*Type{fmt.Sprintf("%s@%d", tp.ID, tp.Version): tp},
	}
	decoded := &Proposal{}
	if err := decoded.Unmarshal(want, resolver); err != nil {
		t.Fatalf("Unmarshal pre-feature bytes: %v", err)
	}
	if decoded.SourceCommitUnixNano != 0 || decoded.SourceTxnID != "" {
		t.Errorf("pre-feature bytes decoded to provenance (%d, %q), want zero values",
			decoded.SourceCommitUnixNano, decoded.SourceTxnID)
	}
}
