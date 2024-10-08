// Code generated by counterfeiter. DO NOT EDIT.
package typesfakes

import (
	"sync"

	"github.com/philborlin/committed/internal/node/types"
)

type FakeLeader struct {
	IsLeaderStub        func() bool
	isLeaderMutex       sync.RWMutex
	isLeaderArgsForCall []struct {
	}
	isLeaderReturns struct {
		result1 bool
	}
	isLeaderReturnsOnCall map[int]struct {
		result1 bool
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeLeader) IsLeader() bool {
	fake.isLeaderMutex.Lock()
	ret, specificReturn := fake.isLeaderReturnsOnCall[len(fake.isLeaderArgsForCall)]
	fake.isLeaderArgsForCall = append(fake.isLeaderArgsForCall, struct {
	}{})
	stub := fake.IsLeaderStub
	fakeReturns := fake.isLeaderReturns
	fake.recordInvocation("IsLeader", []interface{}{})
	fake.isLeaderMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeLeader) IsLeaderCallCount() int {
	fake.isLeaderMutex.RLock()
	defer fake.isLeaderMutex.RUnlock()
	return len(fake.isLeaderArgsForCall)
}

func (fake *FakeLeader) IsLeaderCalls(stub func() bool) {
	fake.isLeaderMutex.Lock()
	defer fake.isLeaderMutex.Unlock()
	fake.IsLeaderStub = stub
}

func (fake *FakeLeader) IsLeaderReturns(result1 bool) {
	fake.isLeaderMutex.Lock()
	defer fake.isLeaderMutex.Unlock()
	fake.IsLeaderStub = nil
	fake.isLeaderReturns = struct {
		result1 bool
	}{result1}
}

func (fake *FakeLeader) IsLeaderReturnsOnCall(i int, result1 bool) {
	fake.isLeaderMutex.Lock()
	defer fake.isLeaderMutex.Unlock()
	fake.IsLeaderStub = nil
	if fake.isLeaderReturnsOnCall == nil {
		fake.isLeaderReturnsOnCall = make(map[int]struct {
			result1 bool
		})
	}
	fake.isLeaderReturnsOnCall[i] = struct {
		result1 bool
	}{result1}
}

func (fake *FakeLeader) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.isLeaderMutex.RLock()
	defer fake.isLeaderMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeLeader) recordInvocation(key string, args []interface{}) {
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

var _ types.Leader = new(FakeLeader)
