// Code generated by counterfeiter. DO NOT EDIT.
package topicfakes

import (
	"context"
	"sync"

	"github.com/philborlin/committed/internal/node/topic"
	"github.com/philborlin/committed/internal/node/types"
)

type FakeReader struct {
	NextStub        func(context.Context) (*types.AcceptedProposal, error)
	nextMutex       sync.RWMutex
	nextArgsForCall []struct {
		arg1 context.Context
	}
	nextReturns struct {
		result1 *types.AcceptedProposal
		result2 error
	}
	nextReturnsOnCall map[int]struct {
		result1 *types.AcceptedProposal
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeReader) Next(arg1 context.Context) (*types.AcceptedProposal, error) {
	fake.nextMutex.Lock()
	ret, specificReturn := fake.nextReturnsOnCall[len(fake.nextArgsForCall)]
	fake.nextArgsForCall = append(fake.nextArgsForCall, struct {
		arg1 context.Context
	}{arg1})
	stub := fake.NextStub
	fakeReturns := fake.nextReturns
	fake.recordInvocation("Next", []interface{}{arg1})
	fake.nextMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeReader) NextCallCount() int {
	fake.nextMutex.RLock()
	defer fake.nextMutex.RUnlock()
	return len(fake.nextArgsForCall)
}

func (fake *FakeReader) NextCalls(stub func(context.Context) (*types.AcceptedProposal, error)) {
	fake.nextMutex.Lock()
	defer fake.nextMutex.Unlock()
	fake.NextStub = stub
}

func (fake *FakeReader) NextArgsForCall(i int) context.Context {
	fake.nextMutex.RLock()
	defer fake.nextMutex.RUnlock()
	argsForCall := fake.nextArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeReader) NextReturns(result1 *types.AcceptedProposal, result2 error) {
	fake.nextMutex.Lock()
	defer fake.nextMutex.Unlock()
	fake.NextStub = nil
	fake.nextReturns = struct {
		result1 *types.AcceptedProposal
		result2 error
	}{result1, result2}
}

func (fake *FakeReader) NextReturnsOnCall(i int, result1 *types.AcceptedProposal, result2 error) {
	fake.nextMutex.Lock()
	defer fake.nextMutex.Unlock()
	fake.NextStub = nil
	if fake.nextReturnsOnCall == nil {
		fake.nextReturnsOnCall = make(map[int]struct {
			result1 *types.AcceptedProposal
			result2 error
		})
	}
	fake.nextReturnsOnCall[i] = struct {
		result1 *types.AcceptedProposal
		result2 error
	}{result1, result2}
}

func (fake *FakeReader) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.nextMutex.RLock()
	defer fake.nextMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeReader) recordInvocation(key string, args []interface{}) {
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

var _ topic.Reader = new(FakeReader)
