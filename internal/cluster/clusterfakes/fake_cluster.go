// Code generated by counterfeiter. DO NOT EDIT.
package clusterfakes

import (
	"context"
	"sync"

	"github.com/philborlin/committed/internal/cluster"
)

type FakeCluster struct {
	AddDatabaseParserStub        func(string, cluster.DatabaseParser)
	addDatabaseParserMutex       sync.RWMutex
	addDatabaseParserArgsForCall []struct {
		arg1 string
		arg2 cluster.DatabaseParser
	}
	AddSyncableParserStub        func(string, cluster.SyncableParser)
	addSyncableParserMutex       sync.RWMutex
	addSyncableParserArgsForCall []struct {
		arg1 string
		arg2 cluster.SyncableParser
	}
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
	IngestStub        func(context.Context, string, cluster.Ingestable) error
	ingestMutex       sync.RWMutex
	ingestArgsForCall []struct {
		arg1 context.Context
		arg2 string
		arg3 cluster.Ingestable
	}
	ingestReturns struct {
		result1 error
	}
	ingestReturnsOnCall map[int]struct {
		result1 error
	}
	ProposeStub        func(*cluster.Proposal) error
	proposeMutex       sync.RWMutex
	proposeArgsForCall []struct {
		arg1 *cluster.Proposal
	}
	proposeReturns struct {
		result1 error
	}
	proposeReturnsOnCall map[int]struct {
		result1 error
	}
	ProposeDatabaseStub        func(*cluster.Configuration) error
	proposeDatabaseMutex       sync.RWMutex
	proposeDatabaseArgsForCall []struct {
		arg1 *cluster.Configuration
	}
	proposeDatabaseReturns struct {
		result1 error
	}
	proposeDatabaseReturnsOnCall map[int]struct {
		result1 error
	}
	ProposeDeleteTypeStub        func(string) error
	proposeDeleteTypeMutex       sync.RWMutex
	proposeDeleteTypeArgsForCall []struct {
		arg1 string
	}
	proposeDeleteTypeReturns struct {
		result1 error
	}
	proposeDeleteTypeReturnsOnCall map[int]struct {
		result1 error
	}
	ProposeIngestableStub        func(*cluster.Configuration) error
	proposeIngestableMutex       sync.RWMutex
	proposeIngestableArgsForCall []struct {
		arg1 *cluster.Configuration
	}
	proposeIngestableReturns struct {
		result1 error
	}
	proposeIngestableReturnsOnCall map[int]struct {
		result1 error
	}
	ProposeSyncableStub        func(*cluster.Configuration) error
	proposeSyncableMutex       sync.RWMutex
	proposeSyncableArgsForCall []struct {
		arg1 *cluster.Configuration
	}
	proposeSyncableReturns struct {
		result1 error
	}
	proposeSyncableReturnsOnCall map[int]struct {
		result1 error
	}
	ProposeTypeStub        func(*cluster.Configuration) error
	proposeTypeMutex       sync.RWMutex
	proposeTypeArgsForCall []struct {
		arg1 *cluster.Configuration
	}
	proposeTypeReturns struct {
		result1 error
	}
	proposeTypeReturnsOnCall map[int]struct {
		result1 error
	}
	SyncStub        func(context.Context, string, cluster.Syncable) error
	syncMutex       sync.RWMutex
	syncArgsForCall []struct {
		arg1 context.Context
		arg2 string
		arg3 cluster.Syncable
	}
	syncReturns struct {
		result1 error
	}
	syncReturnsOnCall map[int]struct {
		result1 error
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

func (fake *FakeCluster) AddDatabaseParser(arg1 string, arg2 cluster.DatabaseParser) {
	fake.addDatabaseParserMutex.Lock()
	fake.addDatabaseParserArgsForCall = append(fake.addDatabaseParserArgsForCall, struct {
		arg1 string
		arg2 cluster.DatabaseParser
	}{arg1, arg2})
	stub := fake.AddDatabaseParserStub
	fake.recordInvocation("AddDatabaseParser", []interface{}{arg1, arg2})
	fake.addDatabaseParserMutex.Unlock()
	if stub != nil {
		fake.AddDatabaseParserStub(arg1, arg2)
	}
}

func (fake *FakeCluster) AddDatabaseParserCallCount() int {
	fake.addDatabaseParserMutex.RLock()
	defer fake.addDatabaseParserMutex.RUnlock()
	return len(fake.addDatabaseParserArgsForCall)
}

func (fake *FakeCluster) AddDatabaseParserCalls(stub func(string, cluster.DatabaseParser)) {
	fake.addDatabaseParserMutex.Lock()
	defer fake.addDatabaseParserMutex.Unlock()
	fake.AddDatabaseParserStub = stub
}

func (fake *FakeCluster) AddDatabaseParserArgsForCall(i int) (string, cluster.DatabaseParser) {
	fake.addDatabaseParserMutex.RLock()
	defer fake.addDatabaseParserMutex.RUnlock()
	argsForCall := fake.addDatabaseParserArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *FakeCluster) AddSyncableParser(arg1 string, arg2 cluster.SyncableParser) {
	fake.addSyncableParserMutex.Lock()
	fake.addSyncableParserArgsForCall = append(fake.addSyncableParserArgsForCall, struct {
		arg1 string
		arg2 cluster.SyncableParser
	}{arg1, arg2})
	stub := fake.AddSyncableParserStub
	fake.recordInvocation("AddSyncableParser", []interface{}{arg1, arg2})
	fake.addSyncableParserMutex.Unlock()
	if stub != nil {
		fake.AddSyncableParserStub(arg1, arg2)
	}
}

func (fake *FakeCluster) AddSyncableParserCallCount() int {
	fake.addSyncableParserMutex.RLock()
	defer fake.addSyncableParserMutex.RUnlock()
	return len(fake.addSyncableParserArgsForCall)
}

func (fake *FakeCluster) AddSyncableParserCalls(stub func(string, cluster.SyncableParser)) {
	fake.addSyncableParserMutex.Lock()
	defer fake.addSyncableParserMutex.Unlock()
	fake.AddSyncableParserStub = stub
}

func (fake *FakeCluster) AddSyncableParserArgsForCall(i int) (string, cluster.SyncableParser) {
	fake.addSyncableParserMutex.RLock()
	defer fake.addSyncableParserMutex.RUnlock()
	argsForCall := fake.addSyncableParserArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *FakeCluster) Close() error {
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

func (fake *FakeCluster) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *FakeCluster) CloseCalls(stub func() error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *FakeCluster) CloseReturns(result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	fake.closeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) CloseReturnsOnCall(i int, result1 error) {
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

func (fake *FakeCluster) Ingest(arg1 context.Context, arg2 string, arg3 cluster.Ingestable) error {
	fake.ingestMutex.Lock()
	ret, specificReturn := fake.ingestReturnsOnCall[len(fake.ingestArgsForCall)]
	fake.ingestArgsForCall = append(fake.ingestArgsForCall, struct {
		arg1 context.Context
		arg2 string
		arg3 cluster.Ingestable
	}{arg1, arg2, arg3})
	stub := fake.IngestStub
	fakeReturns := fake.ingestReturns
	fake.recordInvocation("Ingest", []interface{}{arg1, arg2, arg3})
	fake.ingestMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeCluster) IngestCallCount() int {
	fake.ingestMutex.RLock()
	defer fake.ingestMutex.RUnlock()
	return len(fake.ingestArgsForCall)
}

func (fake *FakeCluster) IngestCalls(stub func(context.Context, string, cluster.Ingestable) error) {
	fake.ingestMutex.Lock()
	defer fake.ingestMutex.Unlock()
	fake.IngestStub = stub
}

func (fake *FakeCluster) IngestArgsForCall(i int) (context.Context, string, cluster.Ingestable) {
	fake.ingestMutex.RLock()
	defer fake.ingestMutex.RUnlock()
	argsForCall := fake.ingestArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeCluster) IngestReturns(result1 error) {
	fake.ingestMutex.Lock()
	defer fake.ingestMutex.Unlock()
	fake.IngestStub = nil
	fake.ingestReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) IngestReturnsOnCall(i int, result1 error) {
	fake.ingestMutex.Lock()
	defer fake.ingestMutex.Unlock()
	fake.IngestStub = nil
	if fake.ingestReturnsOnCall == nil {
		fake.ingestReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.ingestReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) Propose(arg1 *cluster.Proposal) error {
	fake.proposeMutex.Lock()
	ret, specificReturn := fake.proposeReturnsOnCall[len(fake.proposeArgsForCall)]
	fake.proposeArgsForCall = append(fake.proposeArgsForCall, struct {
		arg1 *cluster.Proposal
	}{arg1})
	stub := fake.ProposeStub
	fakeReturns := fake.proposeReturns
	fake.recordInvocation("Propose", []interface{}{arg1})
	fake.proposeMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeCluster) ProposeCallCount() int {
	fake.proposeMutex.RLock()
	defer fake.proposeMutex.RUnlock()
	return len(fake.proposeArgsForCall)
}

func (fake *FakeCluster) ProposeCalls(stub func(*cluster.Proposal) error) {
	fake.proposeMutex.Lock()
	defer fake.proposeMutex.Unlock()
	fake.ProposeStub = stub
}

func (fake *FakeCluster) ProposeArgsForCall(i int) *cluster.Proposal {
	fake.proposeMutex.RLock()
	defer fake.proposeMutex.RUnlock()
	argsForCall := fake.proposeArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeCluster) ProposeReturns(result1 error) {
	fake.proposeMutex.Lock()
	defer fake.proposeMutex.Unlock()
	fake.ProposeStub = nil
	fake.proposeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeReturnsOnCall(i int, result1 error) {
	fake.proposeMutex.Lock()
	defer fake.proposeMutex.Unlock()
	fake.ProposeStub = nil
	if fake.proposeReturnsOnCall == nil {
		fake.proposeReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.proposeReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeDatabase(arg1 *cluster.Configuration) error {
	fake.proposeDatabaseMutex.Lock()
	ret, specificReturn := fake.proposeDatabaseReturnsOnCall[len(fake.proposeDatabaseArgsForCall)]
	fake.proposeDatabaseArgsForCall = append(fake.proposeDatabaseArgsForCall, struct {
		arg1 *cluster.Configuration
	}{arg1})
	stub := fake.ProposeDatabaseStub
	fakeReturns := fake.proposeDatabaseReturns
	fake.recordInvocation("ProposeDatabase", []interface{}{arg1})
	fake.proposeDatabaseMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeCluster) ProposeDatabaseCallCount() int {
	fake.proposeDatabaseMutex.RLock()
	defer fake.proposeDatabaseMutex.RUnlock()
	return len(fake.proposeDatabaseArgsForCall)
}

func (fake *FakeCluster) ProposeDatabaseCalls(stub func(*cluster.Configuration) error) {
	fake.proposeDatabaseMutex.Lock()
	defer fake.proposeDatabaseMutex.Unlock()
	fake.ProposeDatabaseStub = stub
}

func (fake *FakeCluster) ProposeDatabaseArgsForCall(i int) *cluster.Configuration {
	fake.proposeDatabaseMutex.RLock()
	defer fake.proposeDatabaseMutex.RUnlock()
	argsForCall := fake.proposeDatabaseArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeCluster) ProposeDatabaseReturns(result1 error) {
	fake.proposeDatabaseMutex.Lock()
	defer fake.proposeDatabaseMutex.Unlock()
	fake.ProposeDatabaseStub = nil
	fake.proposeDatabaseReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeDatabaseReturnsOnCall(i int, result1 error) {
	fake.proposeDatabaseMutex.Lock()
	defer fake.proposeDatabaseMutex.Unlock()
	fake.ProposeDatabaseStub = nil
	if fake.proposeDatabaseReturnsOnCall == nil {
		fake.proposeDatabaseReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.proposeDatabaseReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeDeleteType(arg1 string) error {
	fake.proposeDeleteTypeMutex.Lock()
	ret, specificReturn := fake.proposeDeleteTypeReturnsOnCall[len(fake.proposeDeleteTypeArgsForCall)]
	fake.proposeDeleteTypeArgsForCall = append(fake.proposeDeleteTypeArgsForCall, struct {
		arg1 string
	}{arg1})
	stub := fake.ProposeDeleteTypeStub
	fakeReturns := fake.proposeDeleteTypeReturns
	fake.recordInvocation("ProposeDeleteType", []interface{}{arg1})
	fake.proposeDeleteTypeMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeCluster) ProposeDeleteTypeCallCount() int {
	fake.proposeDeleteTypeMutex.RLock()
	defer fake.proposeDeleteTypeMutex.RUnlock()
	return len(fake.proposeDeleteTypeArgsForCall)
}

func (fake *FakeCluster) ProposeDeleteTypeCalls(stub func(string) error) {
	fake.proposeDeleteTypeMutex.Lock()
	defer fake.proposeDeleteTypeMutex.Unlock()
	fake.ProposeDeleteTypeStub = stub
}

func (fake *FakeCluster) ProposeDeleteTypeArgsForCall(i int) string {
	fake.proposeDeleteTypeMutex.RLock()
	defer fake.proposeDeleteTypeMutex.RUnlock()
	argsForCall := fake.proposeDeleteTypeArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeCluster) ProposeDeleteTypeReturns(result1 error) {
	fake.proposeDeleteTypeMutex.Lock()
	defer fake.proposeDeleteTypeMutex.Unlock()
	fake.ProposeDeleteTypeStub = nil
	fake.proposeDeleteTypeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeDeleteTypeReturnsOnCall(i int, result1 error) {
	fake.proposeDeleteTypeMutex.Lock()
	defer fake.proposeDeleteTypeMutex.Unlock()
	fake.ProposeDeleteTypeStub = nil
	if fake.proposeDeleteTypeReturnsOnCall == nil {
		fake.proposeDeleteTypeReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.proposeDeleteTypeReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeIngestable(arg1 *cluster.Configuration) error {
	fake.proposeIngestableMutex.Lock()
	ret, specificReturn := fake.proposeIngestableReturnsOnCall[len(fake.proposeIngestableArgsForCall)]
	fake.proposeIngestableArgsForCall = append(fake.proposeIngestableArgsForCall, struct {
		arg1 *cluster.Configuration
	}{arg1})
	stub := fake.ProposeIngestableStub
	fakeReturns := fake.proposeIngestableReturns
	fake.recordInvocation("ProposeIngestable", []interface{}{arg1})
	fake.proposeIngestableMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeCluster) ProposeIngestableCallCount() int {
	fake.proposeIngestableMutex.RLock()
	defer fake.proposeIngestableMutex.RUnlock()
	return len(fake.proposeIngestableArgsForCall)
}

func (fake *FakeCluster) ProposeIngestableCalls(stub func(*cluster.Configuration) error) {
	fake.proposeIngestableMutex.Lock()
	defer fake.proposeIngestableMutex.Unlock()
	fake.ProposeIngestableStub = stub
}

func (fake *FakeCluster) ProposeIngestableArgsForCall(i int) *cluster.Configuration {
	fake.proposeIngestableMutex.RLock()
	defer fake.proposeIngestableMutex.RUnlock()
	argsForCall := fake.proposeIngestableArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeCluster) ProposeIngestableReturns(result1 error) {
	fake.proposeIngestableMutex.Lock()
	defer fake.proposeIngestableMutex.Unlock()
	fake.ProposeIngestableStub = nil
	fake.proposeIngestableReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeIngestableReturnsOnCall(i int, result1 error) {
	fake.proposeIngestableMutex.Lock()
	defer fake.proposeIngestableMutex.Unlock()
	fake.ProposeIngestableStub = nil
	if fake.proposeIngestableReturnsOnCall == nil {
		fake.proposeIngestableReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.proposeIngestableReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeSyncable(arg1 *cluster.Configuration) error {
	fake.proposeSyncableMutex.Lock()
	ret, specificReturn := fake.proposeSyncableReturnsOnCall[len(fake.proposeSyncableArgsForCall)]
	fake.proposeSyncableArgsForCall = append(fake.proposeSyncableArgsForCall, struct {
		arg1 *cluster.Configuration
	}{arg1})
	stub := fake.ProposeSyncableStub
	fakeReturns := fake.proposeSyncableReturns
	fake.recordInvocation("ProposeSyncable", []interface{}{arg1})
	fake.proposeSyncableMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeCluster) ProposeSyncableCallCount() int {
	fake.proposeSyncableMutex.RLock()
	defer fake.proposeSyncableMutex.RUnlock()
	return len(fake.proposeSyncableArgsForCall)
}

func (fake *FakeCluster) ProposeSyncableCalls(stub func(*cluster.Configuration) error) {
	fake.proposeSyncableMutex.Lock()
	defer fake.proposeSyncableMutex.Unlock()
	fake.ProposeSyncableStub = stub
}

func (fake *FakeCluster) ProposeSyncableArgsForCall(i int) *cluster.Configuration {
	fake.proposeSyncableMutex.RLock()
	defer fake.proposeSyncableMutex.RUnlock()
	argsForCall := fake.proposeSyncableArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeCluster) ProposeSyncableReturns(result1 error) {
	fake.proposeSyncableMutex.Lock()
	defer fake.proposeSyncableMutex.Unlock()
	fake.ProposeSyncableStub = nil
	fake.proposeSyncableReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeSyncableReturnsOnCall(i int, result1 error) {
	fake.proposeSyncableMutex.Lock()
	defer fake.proposeSyncableMutex.Unlock()
	fake.ProposeSyncableStub = nil
	if fake.proposeSyncableReturnsOnCall == nil {
		fake.proposeSyncableReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.proposeSyncableReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeType(arg1 *cluster.Configuration) error {
	fake.proposeTypeMutex.Lock()
	ret, specificReturn := fake.proposeTypeReturnsOnCall[len(fake.proposeTypeArgsForCall)]
	fake.proposeTypeArgsForCall = append(fake.proposeTypeArgsForCall, struct {
		arg1 *cluster.Configuration
	}{arg1})
	stub := fake.ProposeTypeStub
	fakeReturns := fake.proposeTypeReturns
	fake.recordInvocation("ProposeType", []interface{}{arg1})
	fake.proposeTypeMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeCluster) ProposeTypeCallCount() int {
	fake.proposeTypeMutex.RLock()
	defer fake.proposeTypeMutex.RUnlock()
	return len(fake.proposeTypeArgsForCall)
}

func (fake *FakeCluster) ProposeTypeCalls(stub func(*cluster.Configuration) error) {
	fake.proposeTypeMutex.Lock()
	defer fake.proposeTypeMutex.Unlock()
	fake.ProposeTypeStub = stub
}

func (fake *FakeCluster) ProposeTypeArgsForCall(i int) *cluster.Configuration {
	fake.proposeTypeMutex.RLock()
	defer fake.proposeTypeMutex.RUnlock()
	argsForCall := fake.proposeTypeArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeCluster) ProposeTypeReturns(result1 error) {
	fake.proposeTypeMutex.Lock()
	defer fake.proposeTypeMutex.Unlock()
	fake.ProposeTypeStub = nil
	fake.proposeTypeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) ProposeTypeReturnsOnCall(i int, result1 error) {
	fake.proposeTypeMutex.Lock()
	defer fake.proposeTypeMutex.Unlock()
	fake.ProposeTypeStub = nil
	if fake.proposeTypeReturnsOnCall == nil {
		fake.proposeTypeReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.proposeTypeReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) Sync(arg1 context.Context, arg2 string, arg3 cluster.Syncable) error {
	fake.syncMutex.Lock()
	ret, specificReturn := fake.syncReturnsOnCall[len(fake.syncArgsForCall)]
	fake.syncArgsForCall = append(fake.syncArgsForCall, struct {
		arg1 context.Context
		arg2 string
		arg3 cluster.Syncable
	}{arg1, arg2, arg3})
	stub := fake.SyncStub
	fakeReturns := fake.syncReturns
	fake.recordInvocation("Sync", []interface{}{arg1, arg2, arg3})
	fake.syncMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeCluster) SyncCallCount() int {
	fake.syncMutex.RLock()
	defer fake.syncMutex.RUnlock()
	return len(fake.syncArgsForCall)
}

func (fake *FakeCluster) SyncCalls(stub func(context.Context, string, cluster.Syncable) error) {
	fake.syncMutex.Lock()
	defer fake.syncMutex.Unlock()
	fake.SyncStub = stub
}

func (fake *FakeCluster) SyncArgsForCall(i int) (context.Context, string, cluster.Syncable) {
	fake.syncMutex.RLock()
	defer fake.syncMutex.RUnlock()
	argsForCall := fake.syncArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeCluster) SyncReturns(result1 error) {
	fake.syncMutex.Lock()
	defer fake.syncMutex.Unlock()
	fake.SyncStub = nil
	fake.syncReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) SyncReturnsOnCall(i int, result1 error) {
	fake.syncMutex.Lock()
	defer fake.syncMutex.Unlock()
	fake.SyncStub = nil
	if fake.syncReturnsOnCall == nil {
		fake.syncReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.syncReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCluster) Type(arg1 string) (*cluster.Type, error) {
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

func (fake *FakeCluster) TypeCallCount() int {
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	return len(fake.typeArgsForCall)
}

func (fake *FakeCluster) TypeCalls(stub func(string) (*cluster.Type, error)) {
	fake.typeMutex.Lock()
	defer fake.typeMutex.Unlock()
	fake.TypeStub = stub
}

func (fake *FakeCluster) TypeArgsForCall(i int) string {
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	argsForCall := fake.typeArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeCluster) TypeReturns(result1 *cluster.Type, result2 error) {
	fake.typeMutex.Lock()
	defer fake.typeMutex.Unlock()
	fake.TypeStub = nil
	fake.typeReturns = struct {
		result1 *cluster.Type
		result2 error
	}{result1, result2}
}

func (fake *FakeCluster) TypeReturnsOnCall(i int, result1 *cluster.Type, result2 error) {
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

func (fake *FakeCluster) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.addDatabaseParserMutex.RLock()
	defer fake.addDatabaseParserMutex.RUnlock()
	fake.addSyncableParserMutex.RLock()
	defer fake.addSyncableParserMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.ingestMutex.RLock()
	defer fake.ingestMutex.RUnlock()
	fake.proposeMutex.RLock()
	defer fake.proposeMutex.RUnlock()
	fake.proposeDatabaseMutex.RLock()
	defer fake.proposeDatabaseMutex.RUnlock()
	fake.proposeDeleteTypeMutex.RLock()
	defer fake.proposeDeleteTypeMutex.RUnlock()
	fake.proposeIngestableMutex.RLock()
	defer fake.proposeIngestableMutex.RUnlock()
	fake.proposeSyncableMutex.RLock()
	defer fake.proposeSyncableMutex.RUnlock()
	fake.proposeTypeMutex.RLock()
	defer fake.proposeTypeMutex.RUnlock()
	fake.syncMutex.RLock()
	defer fake.syncMutex.RUnlock()
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeCluster) recordInvocation(key string, args []interface{}) {
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

var _ cluster.Cluster = new(FakeCluster)
