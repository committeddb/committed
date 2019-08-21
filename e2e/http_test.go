package e2e

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster", func() {
	var (
		err     error
		control *Control
	)

	JustBeforeEach(func() {
		control, err = startup()
		Expect(err).To(BeNil())
	})

	JustAfterEach(func() {
		control.shutdown(true)
	})

	It("starts uninitialized", func() {
		// no databases
		databases, err := control.getDatabases()
		Expect(err).To(BeNil())
		Expect(len(databases)).To(Equal(0))

		// no syncables
		syncables, err := control.getSyncables()
		Expect(err).To(BeNil())
		Expect(len(syncables)).To(Equal(0))

		// no topics
		topics, err := control.getTopics()
		Expect(err).To(BeNil())
		Expect(len(topics)).To(Equal(0))
	})
})
