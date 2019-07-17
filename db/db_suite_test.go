package db

import (
	"testing"

	. "github.com/onsi/ginkgo" // BDD test library
	. "github.com/onsi/gomega" // Matcher
)

func TestDB(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DB Suite")
}
