package e2e

import (
	"testing"

	. "github.com/onsi/ginkgo" // BDD test library
	. "github.com/onsi/gomega" // Matcher
)

func TestE2E(t *testing.T) {
	if !testing.Short() {
		RegisterFailHandler(Fail)
		RunSpecs(t, "End to End Suite")
	}
}