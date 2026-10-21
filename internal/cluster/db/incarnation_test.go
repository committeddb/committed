package db

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

type fakeIncarnationStore struct {
	mu       sync.Mutex
	self     cluster.Incarnation
	recorded map[uint64]cluster.Incarnation
}

func (s *fakeIncarnationStore) LocalIncarnation() (cluster.Incarnation, error) { return s.self, nil }

func (s *fakeIncarnationStore) MemberIncarnation(id uint64) (cluster.Incarnation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.recorded[id]
	return inc, ok
}

func (s *fakeIncarnationStore) record(id uint64, inc cluster.Incarnation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recorded[id] = inc
}

func newInc(t *testing.T) cluster.Incarnation {
	t.Helper()
	inc, err := cluster.NewIncarnation()
	require.NoError(t, err)
	return inc
}

func incarnationIdentity(t *testing.T, store *fakeIncarnationStore) *identity {
	t.Helper()
	return &identity{logger: zap.NewNop(), inc: incarnations{store: store, self: store.self, nodeID: 1, senders: map[uint64]*senderIncarnation{}}}
}

// Once the cluster has recorded a sender's incarnation, only that one is
// admitted.
func TestAdmitIncarnation_RecordedIncarnationOnly(t *testing.T) {
	a, b := newInc(t), newInc(t)
	store := &fakeIncarnationStore{self: newInc(t), recorded: map[uint64]cluster.Incarnation{2: a}}
	id := incarnationIdentity(t, store)
	require.NoError(t, id.AdmitIncarnation(2, a))
	require.ErrorIs(t, id.AdmitIncarnation(2, b), ErrDuplicateNode)
	require.NoError(t, id.AdmitIncarnation(2, a), "the original stays admitted")
}

// Before anything is recorded, a second incarnation for one node id refuses
// both — this node can't tell which is the original — until the cluster
// records one, which is then admitted again.
func TestAdmitIncarnation_TwoUnrecordedIncarnationsAreBothRefused(t *testing.T) {
	a, b := newInc(t), newInc(t)
	store := &fakeIncarnationStore{self: newInc(t), recorded: map[uint64]cluster.Incarnation{}}
	id := incarnationIdentity(t, store)
	require.NoError(t, id.AdmitIncarnation(2, a))
	require.NoError(t, id.AdmitIncarnation(2, a), "one incarnation is admitted as long as it is the only one")
	require.ErrorIs(t, id.AdmitIncarnation(2, b), ErrDuplicateNode)
	require.ErrorIs(t, id.AdmitIncarnation(2, a), ErrDuplicateNode, "the first is refused too once a second appears")
	require.NoError(t, id.AdmitIncarnation(3, b), "other node ids are unaffected")

	store.record(2, a)
	id.inc.senders[2].checkedAt = time.Time{}
	require.NoError(t, id.AdmitIncarnation(2, a), "the recorded incarnation is admitted again")
	require.ErrorIs(t, id.AdmitIncarnation(2, b), ErrDuplicateNode)
}

// A node whose recorded incarnation is another data directory's refuses to
// run; one recorded under its own, or not yet recorded, runs.
func TestCheckOwnIncarnation(t *testing.T) {
	self := newInc(t)
	store := &fakeIncarnationStore{self: self, recorded: map[uint64]cluster.Incarnation{}}
	id := incarnationIdentity(t, store)
	require.NoError(t, id.checkOwnIncarnation())
	store.record(1, self)
	require.NoError(t, id.checkOwnIncarnation())
	store.record(1, newInc(t))
	err := id.checkOwnIncarnation()
	require.Error(t, err)
	require.Contains(t, err.Error(), "another process is running as node 1")
}

// A record that lands between lookups is read before a second incarnation is
// taken for a duplicate: the newcomer is refused alone, and the original —
// now recorded — stays admitted.
func TestAdmitIncarnation_SecondIncarnationRereadsTheRecord(t *testing.T) {
	a, b := newInc(t), newInc(t)
	store := &fakeIncarnationStore{self: newInc(t), recorded: map[uint64]cluster.Incarnation{}}
	id := incarnationIdentity(t, store)
	require.NoError(t, id.AdmitIncarnation(2, a), "first seen, nothing recorded yet")
	store.record(2, a)
	require.ErrorIs(t, id.AdmitIncarnation(2, b), ErrDuplicateNode)
	require.NoError(t, id.AdmitIncarnation(2, a), "the original is not refused")
}
