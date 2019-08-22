package e2e

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster", func() {
	const (
		topicTOML = `[topic]
name = "test1"`
	)

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

	It("adds topics", func() {
		err = control.postTopic(topicTOML)
		Expect(err).To(BeNil())
		topics, err := control.getTopics()
		Expect(err).To(BeNil())
		Expect(len(topics)).To(Equal(1))
		Expect(topics[0]).To(Equal(topicTOML))
	})

	It("loads topic on reload", func() {
		err = control.postTopic(topicTOML)
		Expect(err).To(BeNil())

		control.shutdown(false)
		control, err = startup()
		Expect(err).To(BeNil())

		topics, err := control.getTopics()
		Expect(err).To(BeNil())
		Expect(len(topics)).To(Equal(1))
		Expect(topics[0]).To(Equal(topicTOML))
	})
})
