// Code generated by counterfeiter. DO NOT EDIT.
package bridgefakes

import (
	"context"
	"sync"
	"time"

	"github.com/philborlin/committed/internal/node/bridge"
	"github.com/philborlin/committed/internal/node/types"
)

type FakeBridge struct {
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
	GetSnapshotStub        func() *bridge.Snapshot
	getSnapshotMutex       sync.RWMutex
	getSnapshotArgsForCall []struct {
	}
	getSnapshotReturns struct {
		result1 *bridge.Snapshot
	}
	getSnapshotReturnsOnCall map[int]struct {
		result1 *bridge.Snapshot
	}
	InitStub        func(context.Context, chan<- error, time.Duration) error
	initMutex       sync.RWMutex
	initArgsForCall []struct {
		arg1 context.Context
		arg2 chan<- error
		arg3 time.Duration
	}
	initReturns struct {
		result1 error
	}
	initReturnsOnCall map[int]struct {
		result1 error
	}
	UpdateIndexStub        func(types.Index)
	updateIndexMutex       sync.RWMutex
	updateIndexArgsForCall []struct {
		arg1 types.Index
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeBridge) Close() error {
	fake.closeMutex.Lock()
	ret, specificReturn := fake.closeReturnsOnCall[len(fake.closeArgsForCall)]
	fake.closeArgsForCall = append(fake.closeArgsForCall, struct {
	}{})
	fake.recordInvocation("Close", []interface{}{})
	fake.closeMutex.Unlock()
	if fake.CloseStub != nil {
		return fake.CloseStub()
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.closeReturns
	return fakeReturns.result1
}

func (fake *FakeBridge) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *FakeBridge) CloseCalls(stub func() error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *FakeBridge) CloseReturns(result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	fake.closeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeBridge) CloseReturnsOnCall(i int, result1 error) {
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

func (fake *FakeBridge) GetSnapshot() *bridge.Snapshot {
	fake.getSnapshotMutex.Lock()
	ret, specificReturn := fake.getSnapshotReturnsOnCall[len(fake.getSnapshotArgsForCall)]
	fake.getSnapshotArgsForCall = append(fake.getSnapshotArgsForCall, struct {
	}{})
	fake.recordInvocation("GetSnapshot", []interface{}{})
	fake.getSnapshotMutex.Unlock()
	if fake.GetSnapshotStub != nil {
		return fake.GetSnapshotStub()
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.getSnapshotReturns
	return fakeReturns.result1
}

func (fake *FakeBridge) GetSnapshotCallCount() int {
	fake.getSnapshotMutex.RLock()
	defer fake.getSnapshotMutex.RUnlock()
	return len(fake.getSnapshotArgsForCall)
}

func (fake *FakeBridge) GetSnapshotCalls(stub func() *bridge.Snapshot) {
	fake.getSnapshotMutex.Lock()
	defer fake.getSnapshotMutex.Unlock()
	fake.GetSnapshotStub = stub
}

func (fake *FakeBridge) GetSnapshotReturns(result1 *bridge.Snapshot) {
	fake.getSnapshotMutex.Lock()
	defer fake.getSnapshotMutex.Unlock()
	fake.GetSnapshotStub = nil
	fake.getSnapshotReturns = struct {
		result1 *bridge.Snapshot
	}{result1}
}

func (fake *FakeBridge) GetSnapshotReturnsOnCall(i int, result1 *bridge.Snapshot) {
	fake.getSnapshotMutex.Lock()
	defer fake.getSnapshotMutex.Unlock()
	fake.GetSnapshotStub = nil
	if fake.getSnapshotReturnsOnCall == nil {
		fake.getSnapshotReturnsOnCall = make(map[int]struct {
			result1 *bridge.Snapshot
		})
	}
	fake.getSnapshotReturnsOnCall[i] = struct {
		result1 *bridge.Snapshot
	}{result1}
}

func (fake *FakeBridge) Init(arg1 context.Context, arg2 chan<- error, arg3 time.Duration) error {
	fake.initMutex.Lock()
	ret, specificReturn := fake.initReturnsOnCall[len(fake.initArgsForCall)]
	fake.initArgsForCall = append(fake.initArgsForCall, struct {
		arg1 context.Context
		arg2 chan<- error
		arg3 time.Duration
	}{arg1, arg2, arg3})
	fake.recordInvocation("Init", []interface{}{arg1, arg2, arg3})
	fake.initMutex.Unlock()
	if fake.InitStub != nil {
		return fake.InitStub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.initReturns
	return fakeReturns.result1
}

func (fake *FakeBridge) InitCallCount() int {
	fake.initMutex.RLock()
	defer fake.initMutex.RUnlock()
	return len(fake.initArgsForCall)
}

func (fake *FakeBridge) InitCalls(stub func(context.Context, chan<- error, time.Duration) error) {
	fake.initMutex.Lock()
	defer fake.initMutex.Unlock()
	fake.InitStub = stub
}

func (fake *FakeBridge) InitArgsForCall(i int) (context.Context, chan<- error, time.Duration) {
	fake.initMutex.RLock()
	defer fake.initMutex.RUnlock()
	argsForCall := fake.initArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeBridge) InitReturns(result1 error) {
	fake.initMutex.Lock()
	defer fake.initMutex.Unlock()
	fake.InitStub = nil
	fake.initReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeBridge) InitReturnsOnCall(i int, result1 error) {
	fake.initMutex.Lock()
	defer fake.initMutex.Unlock()
	fake.InitStub = nil
	if fake.initReturnsOnCall == nil {
		fake.initReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.initReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeBridge) UpdateIndex(arg1 types.Index) {
	fake.updateIndexMutex.Lock()
	fake.updateIndexArgsForCall = append(fake.updateIndexArgsForCall, struct {
		arg1 types.Index
	}{arg1})
	fake.recordInvocation("UpdateIndex", []interface{}{arg1})
	fake.updateIndexMutex.Unlock()
	if fake.UpdateIndexStub != nil {
		fake.UpdateIndexStub(arg1)
	}
}

func (fake *FakeBridge) UpdateIndexCallCount() int {
	fake.updateIndexMutex.RLock()
	defer fake.updateIndexMutex.RUnlock()
	return len(fake.updateIndexArgsForCall)
}

func (fake *FakeBridge) UpdateIndexCalls(stub func(types.Index)) {
	fake.updateIndexMutex.Lock()
	defer fake.updateIndexMutex.Unlock()
	fake.UpdateIndexStub = stub
}

func (fake *FakeBridge) UpdateIndexArgsForCall(i int) types.Index {
	fake.updateIndexMutex.RLock()
	defer fake.updateIndexMutex.RUnlock()
	argsForCall := fake.updateIndexArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeBridge) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.getSnapshotMutex.RLock()
	defer fake.getSnapshotMutex.RUnlock()
	fake.initMutex.RLock()
	defer fake.initMutex.RUnlock()
	fake.updateIndexMutex.RLock()
	defer fake.updateIndexMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeBridge) recordInvocation(key string, args []interface{}) {
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

var _ bridge.Bridge = new(FakeBridge)