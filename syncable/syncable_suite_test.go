package syncable

import (
	"testing"

	. "github.com/onsi/ginkgo" // BDD test library
	. "github.com/onsi/gomega" // Matcher
)

func TestSync(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Syncable Suite")
}
