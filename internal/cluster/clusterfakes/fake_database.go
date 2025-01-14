// Code generated by counterfeiter. DO NOT EDIT.
package clusterfakes

import (
	"sync"

	"github.com/philborlin/committed/internal/cluster"
)

type FakeDatabase struct {
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
	GetTypeStub        func() string
	getTypeMutex       sync.RWMutex
	getTypeArgsForCall []struct {
	}
	getTypeReturns struct {
		result1 string
	}
	getTypeReturnsOnCall map[int]struct {
		result1 string
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeDatabase) Close() error {
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

func (fake *FakeDatabase) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *FakeDatabase) CloseCalls(stub func() error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *FakeDatabase) CloseReturns(result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	fake.closeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeDatabase) CloseReturnsOnCall(i int, result1 error) {
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

func (fake *FakeDatabase) GetType() string {
	fake.getTypeMutex.Lock()
	ret, specificReturn := fake.getTypeReturnsOnCall[len(fake.getTypeArgsForCall)]
	fake.getTypeArgsForCall = append(fake.getTypeArgsForCall, struct {
	}{})
	stub := fake.GetTypeStub
	fakeReturns := fake.getTypeReturns
	fake.recordInvocation("GetType", []interface{}{})
	fake.getTypeMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeDatabase) GetTypeCallCount() int {
	fake.getTypeMutex.RLock()
	defer fake.getTypeMutex.RUnlock()
	return len(fake.getTypeArgsForCall)
}

func (fake *FakeDatabase) GetTypeCalls(stub func() string) {
	fake.getTypeMutex.Lock()
	defer fake.getTypeMutex.Unlock()
	fake.GetTypeStub = stub
}

func (fake *FakeDatabase) GetTypeReturns(result1 string) {
	fake.getTypeMutex.Lock()
	defer fake.getTypeMutex.Unlock()
	fake.GetTypeStub = nil
	fake.getTypeReturns = struct {
		result1 string
	}{result1}
}

func (fake *FakeDatabase) GetTypeReturnsOnCall(i int, result1 string) {
	fake.getTypeMutex.Lock()
	defer fake.getTypeMutex.Unlock()
	fake.GetTypeStub = nil
	if fake.getTypeReturnsOnCall == nil {
		fake.getTypeReturnsOnCall = make(map[int]struct {
			result1 string
		})
	}
	fake.getTypeReturnsOnCall[i] = struct {
		result1 string
	}{result1}
}

func (fake *FakeDatabase) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.getTypeMutex.RLock()
	defer fake.getTypeMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeDatabase) recordInvocation(key string, args []interface{}) {
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

var _ cluster.Database = new(FakeDatabase)
