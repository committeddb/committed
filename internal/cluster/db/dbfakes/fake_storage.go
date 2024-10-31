// Code generated by counterfeiter. DO NOT EDIT.
package dbfakes

import (
	"sync"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type FakeStorage struct {
	CloseStub        func() error
	closeMutex       sync.RWMutex
	closeArgsForCall []struct {
	}
	closeReturns struct {
		result1 error
	}
	closeReturnsOnCall map[int]struct {
		result1 error
	}
	DatabaseStub        func(string) (cluster.Database, error)
	databaseMutex       sync.RWMutex
	databaseArgsForCall []struct {
		arg1 string
	}
	databaseReturns struct {
		result1 cluster.Database
		result2 error
	}
	databaseReturnsOnCall map[int]struct {
		result1 cluster.Database
		result2 error
	}
	EntriesStub        func(uint64, uint64, uint64) ([]raftpb.Entry, error)
	entriesMutex       sync.RWMutex
	entriesArgsForCall []struct {
		arg1 uint64
		arg2 uint64
		arg3 uint64
	}
	entriesReturns struct {
		result1 []raftpb.Entry
		result2 error
	}
	entriesReturnsOnCall map[int]struct {
		result1 []raftpb.Entry
		result2 error
	}
	FirstIndexStub        func() (uint64, error)
	firstIndexMutex       sync.RWMutex
	firstIndexArgsForCall []struct {
	}
	firstIndexReturns struct {
		result1 uint64
		result2 error
	}
	firstIndexReturnsOnCall map[int]struct {
		result1 uint64
		result2 error
	}
	InitialStateStub        func() (raftpb.HardState, raftpb.ConfState, error)
	initialStateMutex       sync.RWMutex
	initialStateArgsForCall []struct {
	}
	initialStateReturns struct {
		result1 raftpb.HardState
		result2 raftpb.ConfState
		result3 error
	}
	initialStateReturnsOnCall map[int]struct {
		result1 raftpb.HardState
		result2 raftpb.ConfState
		result3 error
	}
	LastIndexStub        func() (uint64, error)
	lastIndexMutex       sync.RWMutex
	lastIndexArgsForCall []struct {
	}
	lastIndexReturns struct {
		result1 uint64
		result2 error
	}
	lastIndexReturnsOnCall map[int]struct {
		result1 uint64
		result2 error
	}
	PositionStub        func(string) cluster.Position
	positionMutex       sync.RWMutex
	positionArgsForCall []struct {
		arg1 string
	}
	positionReturns struct {
		result1 cluster.Position
	}
	positionReturnsOnCall map[int]struct {
		result1 cluster.Position
	}
	ReaderStub        func(string) db.ProposalReader
	readerMutex       sync.RWMutex
	readerArgsForCall []struct {
		arg1 string
	}
	readerReturns struct {
		result1 db.ProposalReader
	}
	readerReturnsOnCall map[int]struct {
		result1 db.ProposalReader
	}
	SaveStub        func(raftpb.HardState, []raftpb.Entry, raftpb.Snapshot) error
	saveMutex       sync.RWMutex
	saveArgsForCall []struct {
		arg1 raftpb.HardState
		arg2 []raftpb.Entry
		arg3 raftpb.Snapshot
	}
	saveReturns struct {
		result1 error
	}
	saveReturnsOnCall map[int]struct {
		result1 error
	}
	SnapshotStub        func() (raftpb.Snapshot, error)
	snapshotMutex       sync.RWMutex
	snapshotArgsForCall []struct {
	}
	snapshotReturns struct {
		result1 raftpb.Snapshot
		result2 error
	}
	snapshotReturnsOnCall map[int]struct {
		result1 raftpb.Snapshot
		result2 error
	}
	SyncablesStub        func() ([]*cluster.Configuration, error)
	syncablesMutex       sync.RWMutex
	syncablesArgsForCall []struct {
	}
	syncablesReturns struct {
		result1 []*cluster.Configuration
		result2 error
	}
	syncablesReturnsOnCall map[int]struct {
		result1 []*cluster.Configuration
		result2 error
	}
	TermStub        func(uint64) (uint64, error)
	termMutex       sync.RWMutex
	termArgsForCall []struct {
		arg1 uint64
	}
	termReturns struct {
		result1 uint64
		result2 error
	}
	termReturnsOnCall map[int]struct {
		result1 uint64
		result2 error
	}
	TypeStub        func(string) (*cluster.Type, error)
	typeMutex       sync.RWMutex
	typeArgsForCall []struct {
		arg1 string
	}
	typeReturns struct {
		result1 *cluster.Type
		result2 error
	}
	typeReturnsOnCall map[int]struct {
		result1 *cluster.Type
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeStorage) Close() error {
	fake.closeMutex.Lock()
	ret, specificReturn := fake.closeReturnsOnCall[len(fake.closeArgsForCall)]
	fake.closeArgsForCall = append(fake.closeArgsForCall, struct {
	}{})
	stub := fake.CloseStub
	fakeReturns := fake.closeReturns
	fake.recordInvocation("Close", []interface{}{})
	fake.closeMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeStorage) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *FakeStorage) CloseCalls(stub func() error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *FakeStorage) CloseReturns(result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	fake.closeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeStorage) CloseReturnsOnCall(i int, result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	if fake.closeReturnsOnCall == nil {
		fake.closeReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.closeReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeStorage) Database(arg1 string) (cluster.Database, error) {
	fake.databaseMutex.Lock()
	ret, specificReturn := fake.databaseReturnsOnCall[len(fake.databaseArgsForCall)]
	fake.databaseArgsForCall = append(fake.databaseArgsForCall, struct {
		arg1 string
	}{arg1})
	stub := fake.DatabaseStub
	fakeReturns := fake.databaseReturns
	fake.recordInvocation("Database", []interface{}{arg1})
	fake.databaseMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStorage) DatabaseCallCount() int {
	fake.databaseMutex.RLock()
	defer fake.databaseMutex.RUnlock()
	return len(fake.databaseArgsForCall)
}

func (fake *FakeStorage) DatabaseCalls(stub func(string) (cluster.Database, error)) {
	fake.databaseMutex.Lock()
	defer fake.databaseMutex.Unlock()
	fake.DatabaseStub = stub
}

func (fake *FakeStorage) DatabaseArgsForCall(i int) string {
	fake.databaseMutex.RLock()
	defer fake.databaseMutex.RUnlock()
	argsForCall := fake.databaseArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeStorage) DatabaseReturns(result1 cluster.Database, result2 error) {
	fake.databaseMutex.Lock()
	defer fake.databaseMutex.Unlock()
	fake.DatabaseStub = nil
	fake.databaseReturns = struct {
		result1 cluster.Database
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) DatabaseReturnsOnCall(i int, result1 cluster.Database, result2 error) {
	fake.databaseMutex.Lock()
	defer fake.databaseMutex.Unlock()
	fake.DatabaseStub = nil
	if fake.databaseReturnsOnCall == nil {
		fake.databaseReturnsOnCall = make(map[int]struct {
			result1 cluster.Database
			result2 error
		})
	}
	fake.databaseReturnsOnCall[i] = struct {
		result1 cluster.Database
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) Entries(arg1 uint64, arg2 uint64, arg3 uint64) ([]raftpb.Entry, error) {
	fake.entriesMutex.Lock()
	ret, specificReturn := fake.entriesReturnsOnCall[len(fake.entriesArgsForCall)]
	fake.entriesArgsForCall = append(fake.entriesArgsForCall, struct {
		arg1 uint64
		arg2 uint64
		arg3 uint64
	}{arg1, arg2, arg3})
	stub := fake.EntriesStub
	fakeReturns := fake.entriesReturns
	fake.recordInvocation("Entries", []interface{}{arg1, arg2, arg3})
	fake.entriesMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStorage) EntriesCallCount() int {
	fake.entriesMutex.RLock()
	defer fake.entriesMutex.RUnlock()
	return len(fake.entriesArgsForCall)
}

func (fake *FakeStorage) EntriesCalls(stub func(uint64, uint64, uint64) ([]raftpb.Entry, error)) {
	fake.entriesMutex.Lock()
	defer fake.entriesMutex.Unlock()
	fake.EntriesStub = stub
}

func (fake *FakeStorage) EntriesArgsForCall(i int) (uint64, uint64, uint64) {
	fake.entriesMutex.RLock()
	defer fake.entriesMutex.RUnlock()
	argsForCall := fake.entriesArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeStorage) EntriesReturns(result1 []raftpb.Entry, result2 error) {
	fake.entriesMutex.Lock()
	defer fake.entriesMutex.Unlock()
	fake.EntriesStub = nil
	fake.entriesReturns = struct {
		result1 []raftpb.Entry
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) EntriesReturnsOnCall(i int, result1 []raftpb.Entry, result2 error) {
	fake.entriesMutex.Lock()
	defer fake.entriesMutex.Unlock()
	fake.EntriesStub = nil
	if fake.entriesReturnsOnCall == nil {
		fake.entriesReturnsOnCall = make(map[int]struct {
			result1 []raftpb.Entry
			result2 error
		})
	}
	fake.entriesReturnsOnCall[i] = struct {
		result1 []raftpb.Entry
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) FirstIndex() (uint64, error) {
	fake.firstIndexMutex.Lock()
	ret, specificReturn := fake.firstIndexReturnsOnCall[len(fake.firstIndexArgsForCall)]
	fake.firstIndexArgsForCall = append(fake.firstIndexArgsForCall, struct {
	}{})
	stub := fake.FirstIndexStub
	fakeReturns := fake.firstIndexReturns
	fake.recordInvocation("FirstIndex", []interface{}{})
	fake.firstIndexMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStorage) FirstIndexCallCount() int {
	fake.firstIndexMutex.RLock()
	defer fake.firstIndexMutex.RUnlock()
	return len(fake.firstIndexArgsForCall)
}

func (fake *FakeStorage) FirstIndexCalls(stub func() (uint64, error)) {
	fake.firstIndexMutex.Lock()
	defer fake.firstIndexMutex.Unlock()
	fake.FirstIndexStub = stub
}

func (fake *FakeStorage) FirstIndexReturns(result1 uint64, result2 error) {
	fake.firstIndexMutex.Lock()
	defer fake.firstIndexMutex.Unlock()
	fake.FirstIndexStub = nil
	fake.firstIndexReturns = struct {
		result1 uint64
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) FirstIndexReturnsOnCall(i int, result1 uint64, result2 error) {
	fake.firstIndexMutex.Lock()
	defer fake.firstIndexMutex.Unlock()
	fake.FirstIndexStub = nil
	if fake.firstIndexReturnsOnCall == nil {
		fake.firstIndexReturnsOnCall = make(map[int]struct {
			result1 uint64
			result2 error
		})
	}
	fake.firstIndexReturnsOnCall[i] = struct {
		result1 uint64
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	fake.initialStateMutex.Lock()
	ret, specificReturn := fake.initialStateReturnsOnCall[len(fake.initialStateArgsForCall)]
	fake.initialStateArgsForCall = append(fake.initialStateArgsForCall, struct {
	}{})
	stub := fake.InitialStateStub
	fakeReturns := fake.initialStateReturns
	fake.recordInvocation("InitialState", []interface{}{})
	fake.initialStateMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1, ret.result2, ret.result3
	}
	return fakeReturns.result1, fakeReturns.result2, fakeReturns.result3
}

func (fake *FakeStorage) InitialStateCallCount() int {
	fake.initialStateMutex.RLock()
	defer fake.initialStateMutex.RUnlock()
	return len(fake.initialStateArgsForCall)
}

func (fake *FakeStorage) InitialStateCalls(stub func() (raftpb.HardState, raftpb.ConfState, error)) {
	fake.initialStateMutex.Lock()
	defer fake.initialStateMutex.Unlock()
	fake.InitialStateStub = stub
}

func (fake *FakeStorage) InitialStateReturns(result1 raftpb.HardState, result2 raftpb.ConfState, result3 error) {
	fake.initialStateMutex.Lock()
	defer fake.initialStateMutex.Unlock()
	fake.InitialStateStub = nil
	fake.initialStateReturns = struct {
		result1 raftpb.HardState
		result2 raftpb.ConfState
		result3 error
	}{result1, result2, result3}
}

func (fake *FakeStorage) InitialStateReturnsOnCall(i int, result1 raftpb.HardState, result2 raftpb.ConfState, result3 error) {
	fake.initialStateMutex.Lock()
	defer fake.initialStateMutex.Unlock()
	fake.InitialStateStub = nil
	if fake.initialStateReturnsOnCall == nil {
		fake.initialStateReturnsOnCall = make(map[int]struct {
			result1 raftpb.HardState
			result2 raftpb.ConfState
			result3 error
		})
	}
	fake.initialStateReturnsOnCall[i] = struct {
		result1 raftpb.HardState
		result2 raftpb.ConfState
		result3 error
	}{result1, result2, result3}
}

func (fake *FakeStorage) LastIndex() (uint64, error) {
	fake.lastIndexMutex.Lock()
	ret, specificReturn := fake.lastIndexReturnsOnCall[len(fake.lastIndexArgsForCall)]
	fake.lastIndexArgsForCall = append(fake.lastIndexArgsForCall, struct {
	}{})
	stub := fake.LastIndexStub
	fakeReturns := fake.lastIndexReturns
	fake.recordInvocation("LastIndex", []interface{}{})
	fake.lastIndexMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStorage) LastIndexCallCount() int {
	fake.lastIndexMutex.RLock()
	defer fake.lastIndexMutex.RUnlock()
	return len(fake.lastIndexArgsForCall)
}

func (fake *FakeStorage) LastIndexCalls(stub func() (uint64, error)) {
	fake.lastIndexMutex.Lock()
	defer fake.lastIndexMutex.Unlock()
	fake.LastIndexStub = stub
}

func (fake *FakeStorage) LastIndexReturns(result1 uint64, result2 error) {
	fake.lastIndexMutex.Lock()
	defer fake.lastIndexMutex.Unlock()
	fake.LastIndexStub = nil
	fake.lastIndexReturns = struct {
		result1 uint64
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) LastIndexReturnsOnCall(i int, result1 uint64, result2 error) {
	fake.lastIndexMutex.Lock()
	defer fake.lastIndexMutex.Unlock()
	fake.LastIndexStub = nil
	if fake.lastIndexReturnsOnCall == nil {
		fake.lastIndexReturnsOnCall = make(map[int]struct {
			result1 uint64
			result2 error
		})
	}
	fake.lastIndexReturnsOnCall[i] = struct {
		result1 uint64
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) Position(arg1 string) cluster.Position {
	fake.positionMutex.Lock()
	ret, specificReturn := fake.positionReturnsOnCall[len(fake.positionArgsForCall)]
	fake.positionArgsForCall = append(fake.positionArgsForCall, struct {
		arg1 string
	}{arg1})
	stub := fake.PositionStub
	fakeReturns := fake.positionReturns
	fake.recordInvocation("Position", []interface{}{arg1})
	fake.positionMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeStorage) PositionCallCount() int {
	fake.positionMutex.RLock()
	defer fake.positionMutex.RUnlock()
	return len(fake.positionArgsForCall)
}

func (fake *FakeStorage) PositionCalls(stub func(string) cluster.Position) {
	fake.positionMutex.Lock()
	defer fake.positionMutex.Unlock()
	fake.PositionStub = stub
}

func (fake *FakeStorage) PositionArgsForCall(i int) string {
	fake.positionMutex.RLock()
	defer fake.positionMutex.RUnlock()
	argsForCall := fake.positionArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeStorage) PositionReturns(result1 cluster.Position) {
	fake.positionMutex.Lock()
	defer fake.positionMutex.Unlock()
	fake.PositionStub = nil
	fake.positionReturns = struct {
		result1 cluster.Position
	}{result1}
}

func (fake *FakeStorage) PositionReturnsOnCall(i int, result1 cluster.Position) {
	fake.positionMutex.Lock()
	defer fake.positionMutex.Unlock()
	fake.PositionStub = nil
	if fake.positionReturnsOnCall == nil {
		fake.positionReturnsOnCall = make(map[int]struct {
			result1 cluster.Position
		})
	}
	fake.positionReturnsOnCall[i] = struct {
		result1 cluster.Position
	}{result1}
}

func (fake *FakeStorage) Reader(arg1 string) db.ProposalReader {
	fake.readerMutex.Lock()
	ret, specificReturn := fake.readerReturnsOnCall[len(fake.readerArgsForCall)]
	fake.readerArgsForCall = append(fake.readerArgsForCall, struct {
		arg1 string
	}{arg1})
	stub := fake.ReaderStub
	fakeReturns := fake.readerReturns
	fake.recordInvocation("Reader", []interface{}{arg1})
	fake.readerMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeStorage) ReaderCallCount() int {
	fake.readerMutex.RLock()
	defer fake.readerMutex.RUnlock()
	return len(fake.readerArgsForCall)
}

func (fake *FakeStorage) ReaderCalls(stub func(string) db.ProposalReader) {
	fake.readerMutex.Lock()
	defer fake.readerMutex.Unlock()
	fake.ReaderStub = stub
}

func (fake *FakeStorage) ReaderArgsForCall(i int) string {
	fake.readerMutex.RLock()
	defer fake.readerMutex.RUnlock()
	argsForCall := fake.readerArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeStorage) ReaderReturns(result1 db.ProposalReader) {
	fake.readerMutex.Lock()
	defer fake.readerMutex.Unlock()
	fake.ReaderStub = nil
	fake.readerReturns = struct {
		result1 db.ProposalReader
	}{result1}
}

func (fake *FakeStorage) ReaderReturnsOnCall(i int, result1 db.ProposalReader) {
	fake.readerMutex.Lock()
	defer fake.readerMutex.Unlock()
	fake.ReaderStub = nil
	if fake.readerReturnsOnCall == nil {
		fake.readerReturnsOnCall = make(map[int]struct {
			result1 db.ProposalReader
		})
	}
	fake.readerReturnsOnCall[i] = struct {
		result1 db.ProposalReader
	}{result1}
}

func (fake *FakeStorage) Save(arg1 raftpb.HardState, arg2 []raftpb.Entry, arg3 raftpb.Snapshot) error {
	var arg2Copy []raftpb.Entry
	if arg2 != nil {
		arg2Copy = make([]raftpb.Entry, len(arg2))
		copy(arg2Copy, arg2)
	}
	fake.saveMutex.Lock()
	ret, specificReturn := fake.saveReturnsOnCall[len(fake.saveArgsForCall)]
	fake.saveArgsForCall = append(fake.saveArgsForCall, struct {
		arg1 raftpb.HardState
		arg2 []raftpb.Entry
		arg3 raftpb.Snapshot
	}{arg1, arg2Copy, arg3})
	stub := fake.SaveStub
	fakeReturns := fake.saveReturns
	fake.recordInvocation("Save", []interface{}{arg1, arg2Copy, arg3})
	fake.saveMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeStorage) SaveCallCount() int {
	fake.saveMutex.RLock()
	defer fake.saveMutex.RUnlock()
	return len(fake.saveArgsForCall)
}

func (fake *FakeStorage) SaveCalls(stub func(raftpb.HardState, []raftpb.Entry, raftpb.Snapshot) error) {
	fake.saveMutex.Lock()
	defer fake.saveMutex.Unlock()
	fake.SaveStub = stub
}

func (fake *FakeStorage) SaveArgsForCall(i int) (raftpb.HardState, []raftpb.Entry, raftpb.Snapshot) {
	fake.saveMutex.RLock()
	defer fake.saveMutex.RUnlock()
	argsForCall := fake.saveArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeStorage) SaveReturns(result1 error) {
	fake.saveMutex.Lock()
	defer fake.saveMutex.Unlock()
	fake.SaveStub = nil
	fake.saveReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeStorage) SaveReturnsOnCall(i int, result1 error) {
	fake.saveMutex.Lock()
	defer fake.saveMutex.Unlock()
	fake.SaveStub = nil
	if fake.saveReturnsOnCall == nil {
		fake.saveReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.saveReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeStorage) Snapshot() (raftpb.Snapshot, error) {
	fake.snapshotMutex.Lock()
	ret, specificReturn := fake.snapshotReturnsOnCall[len(fake.snapshotArgsForCall)]
	fake.snapshotArgsForCall = append(fake.snapshotArgsForCall, struct {
	}{})
	stub := fake.SnapshotStub
	fakeReturns := fake.snapshotReturns
	fake.recordInvocation("Snapshot", []interface{}{})
	fake.snapshotMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStorage) SnapshotCallCount() int {
	fake.snapshotMutex.RLock()
	defer fake.snapshotMutex.RUnlock()
	return len(fake.snapshotArgsForCall)
}

func (fake *FakeStorage) SnapshotCalls(stub func() (raftpb.Snapshot, error)) {
	fake.snapshotMutex.Lock()
	defer fake.snapshotMutex.Unlock()
	fake.SnapshotStub = stub
}

func (fake *FakeStorage) SnapshotReturns(result1 raftpb.Snapshot, result2 error) {
	fake.snapshotMutex.Lock()
	defer fake.snapshotMutex.Unlock()
	fake.SnapshotStub = nil
	fake.snapshotReturns = struct {
		result1 raftpb.Snapshot
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) SnapshotReturnsOnCall(i int, result1 raftpb.Snapshot, result2 error) {
	fake.snapshotMutex.Lock()
	defer fake.snapshotMutex.Unlock()
	fake.SnapshotStub = nil
	if fake.snapshotReturnsOnCall == nil {
		fake.snapshotReturnsOnCall = make(map[int]struct {
			result1 raftpb.Snapshot
			result2 error
		})
	}
	fake.snapshotReturnsOnCall[i] = struct {
		result1 raftpb.Snapshot
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) Syncables() ([]*cluster.Configuration, error) {
	fake.syncablesMutex.Lock()
	ret, specificReturn := fake.syncablesReturnsOnCall[len(fake.syncablesArgsForCall)]
	fake.syncablesArgsForCall = append(fake.syncablesArgsForCall, struct {
	}{})
	stub := fake.SyncablesStub
	fakeReturns := fake.syncablesReturns
	fake.recordInvocation("Syncables", []interface{}{})
	fake.syncablesMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStorage) SyncablesCallCount() int {
	fake.syncablesMutex.RLock()
	defer fake.syncablesMutex.RUnlock()
	return len(fake.syncablesArgsForCall)
}

func (fake *FakeStorage) SyncablesCalls(stub func() ([]*cluster.Configuration, error)) {
	fake.syncablesMutex.Lock()
	defer fake.syncablesMutex.Unlock()
	fake.SyncablesStub = stub
}

func (fake *FakeStorage) SyncablesReturns(result1 []*cluster.Configuration, result2 error) {
	fake.syncablesMutex.Lock()
	defer fake.syncablesMutex.Unlock()
	fake.SyncablesStub = nil
	fake.syncablesReturns = struct {
		result1 []*cluster.Configuration
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) SyncablesReturnsOnCall(i int, result1 []*cluster.Configuration, result2 error) {
	fake.syncablesMutex.Lock()
	defer fake.syncablesMutex.Unlock()
	fake.SyncablesStub = nil
	if fake.syncablesReturnsOnCall == nil {
		fake.syncablesReturnsOnCall = make(map[int]struct {
			result1 []*cluster.Configuration
			result2 error
		})
	}
	fake.syncablesReturnsOnCall[i] = struct {
		result1 []*cluster.Configuration
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) Term(arg1 uint64) (uint64, error) {
	fake.termMutex.Lock()
	ret, specificReturn := fake.termReturnsOnCall[len(fake.termArgsForCall)]
	fake.termArgsForCall = append(fake.termArgsForCall, struct {
		arg1 uint64
	}{arg1})
	stub := fake.TermStub
	fakeReturns := fake.termReturns
	fake.recordInvocation("Term", []interface{}{arg1})
	fake.termMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStorage) TermCallCount() int {
	fake.termMutex.RLock()
	defer fake.termMutex.RUnlock()
	return len(fake.termArgsForCall)
}

func (fake *FakeStorage) TermCalls(stub func(uint64) (uint64, error)) {
	fake.termMutex.Lock()
	defer fake.termMutex.Unlock()
	fake.TermStub = stub
}

func (fake *FakeStorage) TermArgsForCall(i int) uint64 {
	fake.termMutex.RLock()
	defer fake.termMutex.RUnlock()
	argsForCall := fake.termArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeStorage) TermReturns(result1 uint64, result2 error) {
	fake.termMutex.Lock()
	defer fake.termMutex.Unlock()
	fake.TermStub = nil
	fake.termReturns = struct {
		result1 uint64
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) TermReturnsOnCall(i int, result1 uint64, result2 error) {
	fake.termMutex.Lock()
	defer fake.termMutex.Unlock()
	fake.TermStub = nil
	if fake.termReturnsOnCall == nil {
		fake.termReturnsOnCall = make(map[int]struct {
			result1 uint64
			result2 error
		})
	}
	fake.termReturnsOnCall[i] = struct {
		result1 uint64
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) Type(arg1 string) (*cluster.Type, error) {
	fake.typeMutex.Lock()
	ret, specificReturn := fake.typeReturnsOnCall[len(fake.typeArgsForCall)]
	fake.typeArgsForCall = append(fake.typeArgsForCall, struct {
		arg1 string
	}{arg1})
	stub := fake.TypeStub
	fakeReturns := fake.typeReturns
	fake.recordInvocation("Type", []interface{}{arg1})
	fake.typeMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeStorage) TypeCallCount() int {
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	return len(fake.typeArgsForCall)
}

func (fake *FakeStorage) TypeCalls(stub func(string) (*cluster.Type, error)) {
	fake.typeMutex.Lock()
	defer fake.typeMutex.Unlock()
	fake.TypeStub = stub
}

func (fake *FakeStorage) TypeArgsForCall(i int) string {
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	argsForCall := fake.typeArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeStorage) TypeReturns(result1 *cluster.Type, result2 error) {
	fake.typeMutex.Lock()
	defer fake.typeMutex.Unlock()
	fake.TypeStub = nil
	fake.typeReturns = struct {
		result1 *cluster.Type
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) TypeReturnsOnCall(i int, result1 *cluster.Type, result2 error) {
	fake.typeMutex.Lock()
	defer fake.typeMutex.Unlock()
	fake.TypeStub = nil
	if fake.typeReturnsOnCall == nil {
		fake.typeReturnsOnCall = make(map[int]struct {
			result1 *cluster.Type
			result2 error
		})
	}
	fake.typeReturnsOnCall[i] = struct {
		result1 *cluster.Type
		result2 error
	}{result1, result2}
}

func (fake *FakeStorage) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.databaseMutex.RLock()
	defer fake.databaseMutex.RUnlock()
	fake.entriesMutex.RLock()
	defer fake.entriesMutex.RUnlock()
	fake.firstIndexMutex.RLock()
	defer fake.firstIndexMutex.RUnlock()
	fake.initialStateMutex.RLock()
	defer fake.initialStateMutex.RUnlock()
	fake.lastIndexMutex.RLock()
	defer fake.lastIndexMutex.RUnlock()
	fake.positionMutex.RLock()
	defer fake.positionMutex.RUnlock()
	fake.readerMutex.RLock()
	defer fake.readerMutex.RUnlock()
	fake.saveMutex.RLock()
	defer fake.saveMutex.RUnlock()
	fake.snapshotMutex.RLock()
	defer fake.snapshotMutex.RUnlock()
	fake.syncablesMutex.RLock()
	defer fake.syncablesMutex.RUnlock()
	fake.termMutex.RLock()
	defer fake.termMutex.RUnlock()
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeStorage) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ db.Storage = new(FakeStorage)
