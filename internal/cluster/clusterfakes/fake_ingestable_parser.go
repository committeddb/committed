// Code generated by counterfeiter. DO NOT EDIT.
package clusterfakes

import (
	"sync"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/spf13/viper"
)

type FakeIngestableParser struct {
	ParseStub        func(*viper.Viper) (cluster.Ingestable, error)
	parseMutex       sync.RWMutex
	parseArgsForCall []struct {
		arg1 *viper.Viper
	}
	parseReturns struct {
		result1 cluster.Ingestable
		result2 error
	}
	parseReturnsOnCall map[int]struct {
		result1 cluster.Ingestable
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeIngestableParser) Parse(arg1 *viper.Viper) (cluster.Ingestable, error) {
	fake.parseMutex.Lock()
	ret, specificReturn := fake.parseReturnsOnCall[len(fake.parseArgsForCall)]
	fake.parseArgsForCall = append(fake.parseArgsForCall, struct {
		arg1 *viper.Viper
	}{arg1})
	stub := fake.ParseStub
	fakeReturns := fake.parseReturns
	fake.recordInvocation("Parse", []interface{}{arg1})
	fake.parseMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeIngestableParser) ParseCallCount() int {
	fake.parseMutex.RLock()
	defer fake.parseMutex.RUnlock()
	return len(fake.parseArgsForCall)
}

func (fake *FakeIngestableParser) ParseCalls(stub func(*viper.Viper) (cluster.Ingestable, error)) {
	fake.parseMutex.Lock()
	defer fake.parseMutex.Unlock()
	fake.ParseStub = stub
}

func (fake *FakeIngestableParser) ParseArgsForCall(i int) *viper.Viper {
	fake.parseMutex.RLock()
	defer fake.parseMutex.RUnlock()
	argsForCall := fake.parseArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeIngestableParser) ParseReturns(result1 cluster.Ingestable, result2 error) {
	fake.parseMutex.Lock()
	defer fake.parseMutex.Unlock()
	fake.ParseStub = nil
	fake.parseReturns = struct {
		result1 cluster.Ingestable
		result2 error
	}{result1, result2}
}

func (fake *FakeIngestableParser) ParseReturnsOnCall(i int, result1 cluster.Ingestable, result2 error) {
	fake.parseMutex.Lock()
	defer fake.parseMutex.Unlock()
	fake.ParseStub = nil
	if fake.parseReturnsOnCall == nil {
		fake.parseReturnsOnCall = make(map[int]struct {
			result1 cluster.Ingestable
			result2 error
		})
	}
	fake.parseReturnsOnCall[i] = struct {
		result1 cluster.Ingestable
		result2 error
	}{result1, result2}
}

func (fake *FakeIngestableParser) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.parseMutex.RLock()
	defer fake.parseMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeIngestableParser) recordInvocation(key string, args []interface{}) {
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

var _ cluster.IngestableParser = new(FakeIngestableParser)
