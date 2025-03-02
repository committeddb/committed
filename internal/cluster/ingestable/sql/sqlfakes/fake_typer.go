// Code generated by counterfeiter. DO NOT EDIT.
package sqlfakes

import (
	"sync"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
)

type FakeTyper struct {
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

func (fake *FakeTyper) Type(arg1 string) (*cluster.Type, error) {
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

func (fake *FakeTyper) TypeCallCount() int {
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	return len(fake.typeArgsForCall)
}

func (fake *FakeTyper) TypeCalls(stub func(string) (*cluster.Type, error)) {
	fake.typeMutex.Lock()
	defer fake.typeMutex.Unlock()
	fake.TypeStub = stub
}

func (fake *FakeTyper) TypeArgsForCall(i int) string {
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	argsForCall := fake.typeArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeTyper) TypeReturns(result1 *cluster.Type, result2 error) {
	fake.typeMutex.Lock()
	defer fake.typeMutex.Unlock()
	fake.TypeStub = nil
	fake.typeReturns = struct {
		result1 *cluster.Type
		result2 error
	}{result1, result2}
}

func (fake *FakeTyper) TypeReturnsOnCall(i int, result1 *cluster.Type, result2 error) {
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

func (fake *FakeTyper) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeTyper) recordInvocation(key string, args []interface{}) {
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

var _ sql.Typer = new(FakeTyper)
