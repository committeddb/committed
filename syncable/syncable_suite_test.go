package syncable

import (
	"testing"

	. "github.com/onsi/ginkgo" // BDD test library
	. "github.com/onsi/gomega" // Matcher
)

func TestSyncable(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Syncable Suite")
}
