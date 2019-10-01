package sql

import (
	"testing"

	. "github.com/onsi/ginkgo" // BDD test library
	. "github.com/onsi/gomega" // Matcher
)

func TestSQL(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sql Suite")
}
