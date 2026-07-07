package wal_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

// TODO Test that we skip malformed Proposals (or pb.Entry structs that aren't proposals)
// TODO Test that we skip system Proposals (types, configurations, read indexes, etc.)

func TestReader(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"simple":  {inputs: [][]string{{"foo"}}},
		"two":     {inputs: [][]string{{"foo", "bar"}}},
		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStorage(t, nil)
			defer s.Cleanup()

			pc := newProposalCreatorForString(t, s)
			ps := pc.createAndSaveProposals(t, tt.inputs)

			r := s.Reader("qux")
			// The reader filters committed's internal entries (the "string"
			// type registration included — see cluster.IsInternal), so it
			// surfaces only the user-data proposals, in order.

			for _, expected := range ps {
				got, err := r.Read()
				require.Equal(t, nil, err)
				require.Equal(t, expected.Entities, got.Entities)
			}
		})
	}
}

func TestReaderSkipsSyncableIndexes(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"simple":  {inputs: [][]string{{"foo"}}},
		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			id := "foo"
			s := NewStorage(t, nil)
			defer s.Cleanup()

			pc := newProposalCreatorForString(t, s)

			ps := make([]*cluster.Proposal, 0, 2*len(tt.inputs))
			for _, input := range tt.inputs {
				ps = append(ps, createProposal(input))
				ps = append(ps, createSyncableIndexProposal(t, id))
			}
			pc.saveProposals(t, ps)

			psAssert := []*cluster.Proposal{}
			for _, p := range ps {
				if !cluster.IsSyncableIndex(p.Entities[0].Type.ID) {
					psAssert = append(psAssert, p)
				}
			}

			r := s.Reader(id)
			// The reader filters committed's internal entries — the "string"
			// type registration AND the interleaved syncable-index bumps (see
			// cluster.IsInternal) — so it surfaces only the user-data
			// proposals (psAssert), in order.

			for _, expected := range psAssert {
				got, err := r.Read()
				require.Equal(t, nil, err)
				require.Equal(t, expected.Entities, got.Entities)
			}
		})
	}
}

func TestEOF(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"empty":  {inputs: [][]string{}},
		"simple": {inputs: [][]string{{"foo"}}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStorage(t, nil)
			defer s.Cleanup()
			// Register the "string" type at index 1; entity proposals
			// follow starting at index 2.
			s.RegisterType(t, "string", 1, 1)

			r := s.Reader("qux")
			// The reader filters committed's internal entries (the type
			// registration at index 1 included — see cluster.IsInternal), so
			// it surfaces only the user-data proposals below, in order.

			ps := createProposals(tt.inputs)
			for i, p := range ps {
				saveProposal(t, p, s, 1, 2+uint64(i))

				got, err := r.Read()
				require.Equal(t, nil, err)
				require.Equal(t, p.Entities, got.Entities)
			}

			_, err := r.Read()
			require.Equal(t, io.EOF, err)
		})
	}
}

func TestResumeReader(t *testing.T) {
	tests := map[string]struct {
		firstRead  [][]string
		secondRead [][]string
	}{
		"one-zero": {firstRead: [][]string{{"foo"}}, secondRead: [][]string{}},
		"zero-one": {firstRead: [][]string{}, secondRead: [][]string{{"foo"}}},
		"one-one":  {firstRead: [][]string{{"foo"}}, secondRead: [][]string{{"bar"}}},
		"two-one":  {firstRead: [][]string{{"foo"}, {"bar"}}, secondRead: [][]string{{"baz"}}},
		"two-two":  {firstRead: [][]string{{"foo"}, {"bar"}}, secondRead: [][]string{{"baz"}, {"qux"}}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			id := "qux"
			s := NewStorage(t, nil)
			defer s.Cleanup()

			pc := newProposalCreatorForString(t, s)
			ps := pc.createAndSaveProposals(t, tt.firstRead)

			r := s.Reader(id)
			// The reader filters committed's internal entries (the "string"
			// type registration, conf-changes, syncable-index bumps — see
			// cluster.IsInternal), so it surfaces only the user-data
			// proposals. Track the last user-proposal index read; it becomes
			// the persisted syncable position for the second read pass. Stays
			// 0 when the first read is empty.
			var index uint64
			for _, expected := range ps {
				got, err := r.Read()
				require.Equal(t, nil, err)
				index = got.Index
				require.Equal(t, expected.Entities, got.Entities)
			}

			pc.saveProposals(t, []*cluster.Proposal{createSyncableIndexProposalWithIndex(t, id, index)})

			ps = pc.createAndSaveProposals(t, tt.secondRead)
			r = s.Reader(id)

			for _, expected := range ps {
				got, err := r.Read()
				require.Equal(t, nil, err)
				require.Equal(t, expected.Entities, got.Entities)
			}
		})
	}
}

type ProposalCreator struct {
	currentIndex uint64
	currentTerm  uint64
	s            *StorageWrapper
}

func NewProposalCreator(s *StorageWrapper) *ProposalCreator {
	return &ProposalCreator{currentIndex: uint64(6), currentTerm: uint64(6), s: s}
}

// newProposalCreatorForString registers the "string" type used by
// createProposal via the real apply path (consuming one raft index),
// then returns a ProposalCreator ready to append entity proposals after
// it. Tests using createProposal/createProposals must go through this
// constructor so that ApplyCommitted can resolve the type when the
// entity proposals are applied.
func newProposalCreatorForString(t *testing.T, s *StorageWrapper) *ProposalCreator {
	pc := NewProposalCreator(s)
	pc.currentIndex++
	s.RegisterType(t, "string", pc.currentTerm, pc.currentIndex)
	return pc
}

func (c *ProposalCreator) saveProposals(t *testing.T, ps []*cluster.Proposal) []*cluster.Proposal {
	for _, p := range ps {
		c.currentIndex += 1
		configEntry := &pb.Entry{
			Term:  proto.Uint64(c.currentTerm),
			Index: proto.Uint64(c.currentIndex),
			Type:  pb.EntryConfChange.Enum(),
		}
		err := c.s.Save(&defaultHardState, []*pb.Entry{configEntry}, &defaultSnap)
		require.Equal(t, nil, err)
		c.currentIndex += 1
		saveProposal(t, p, c.s, c.currentTerm, c.currentIndex)
	}

	return ps
}

func (c *ProposalCreator) createAndSaveProposals(t *testing.T, inputs [][]string) []*cluster.Proposal {
	ps := createProposals(inputs)
	return c.saveProposals(t, ps)
}

func createProposals(input [][]string) []*cluster.Proposal {
	ps := make([]*cluster.Proposal, 0, len(input))

	for _, entities := range input {
		ps = append(ps, createProposal(entities))
	}

	return ps
}

func createProposal(input []string) *cluster.Proposal {
	proposal := &cluster.Proposal{}
	for _, entity := range input {
		logEntity := &cluster.Entity{
			// Must match what StorageWrapper.RegisterType writes, so
			// that proposals produced by this helper compare equal to
			// proposals read back after apply-path hydration.
			Type: &cluster.Type{ID: "string", Name: "string", Version: 1},
			Key:  []byte(entity),
			Data: []byte(entity),
		}
		proposal.Entities = append(proposal.Entities, logEntity)
	}
	return proposal
}

func createSyncableIndexProposal(t *testing.T, id string) *cluster.Proposal {
	return createSyncableIndexProposalWithIndex(t, id, 0)
}

func createSyncableIndexProposalWithIndex(t *testing.T, id string, index uint64) *cluster.Proposal {
	e, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: id, Index: index})
	require.Nil(t, err)
	return &cluster.Proposal{Entities: []*cluster.Entity{e}}
}

// TestReaderCorruptCheckpointLogsAndRewinds is the regression for a silent
// rewind: a corrupt persisted checkpoint (undecodable bytes) must not restart a
// syncable from the head of the log with zero operator signal. Reader still
// rewinds to 0 (the completeness-safe fallback — we can't know how far it got),
// but now logs loudly so the full re-sync (and any duplicate downstream
// deliveries on a non-idempotent sink) is visible.
func TestReaderCorruptCheckpointLogsAndRewinds(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	// Precondition: these bytes must be undecodable as a SyncableIndex, else the
	// test would not exercise the corrupt path.
	corrupt := []byte{0x0a, 0x05, 0x01} // field 1, length-delimited, claims 5 bytes; 1 present
	require.Error(t, (&cluster.SyncableIndex{}).Unmarshal(corrupt), "test bytes must fail to decode")
	require.NoError(t, s.PutRawSyncableIndexForTest("s1", corrupt))

	core, logs := observer.New(zap.ErrorLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	r := s.Reader("s1")
	require.NotNil(t, r)

	entries := logs.FilterMessageSnippet("corrupt").All()
	require.Len(t, entries, 1, "a corrupt checkpoint must be logged loudly, not silently rewound")
	require.Equal(t, zap.ErrorLevel, entries[0].Level)
	require.Equal(t, "s1", entries[0].ContextMap()["syncable"])
}

// TestReaderFreshCheckpointDoesNotLog: a syncable that has never checkpointed is
// a legitimate start-from-head and must NOT be reported as corrupt.
func TestReaderFreshCheckpointDoesNotLog(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	core, logs := observer.New(zap.ErrorLevel)
	defer zap.ReplaceGlobals(zap.New(core))()

	r := s.Reader("never-checkpointed")
	require.NotNil(t, r)
	require.Zero(t, logs.FilterMessageSnippet("corrupt").Len(),
		"a fresh start must not be logged as corrupt")
}
