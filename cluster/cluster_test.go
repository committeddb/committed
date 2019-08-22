package cluster

import (
	"github.com/philborlin/committed/bridge/bridgefakes"
	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/topic"
	"github.com/philborlin/committed/topic/topicfakes"
	"github.com/philborlin/committed/types"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster", func() {
	var (
		fakeBridge        *bridgefakes.FakeBridge
		fakeBridgeFactory *bridgefakes.FakeFactory
	)

	JustBeforeEach(func() {
		fakeBridge = &bridgefakes.FakeBridge{}
		fakeBridgeFactory = &bridgefakes.FakeFactory{}
		fakeBridgeFactory.NewReturns(fakeBridge, nil)
		bridgeFactory = fakeBridgeFactory
	})

	Describe("route()", func() {
		var (
			c         *Cluster
			fakeTopic *topicfakes.FakeTopic
		)

		JustBeforeEach(func() {
			data := &Data{
				Databases: make(map[string]syncable.Database),
				Syncables: make(map[string]syncable.Syncable),
				Topics:    make(map[string]topic.Topic),
			}
			toml := &TOML{}
			c = &Cluster{Data: data, TOML: toml, dataDir: "data/"}
			fakeTopic = &topicfakes.FakeTopic{}
		})

		It("should correctly add a Topic", func() {
			toml := `[topic]
			name = "test1"`
			ap := &types.AcceptedProposal{Topic: "topic", Data: []byte(toml)}
			Expect(len(c.Data.Topics)).To(Equal(0))

			err := c.route(ap)
			Expect(err).To(BeNil())
			Expect(len(c.Data.Topics)).To(Equal(1))
			Expect(c.Data.Topics).To(HaveKey("test1"))
			Expect(c.Data.Topics["test1"].Name()).To(Equal("test1"))
		})

		It("should correctly add a Database", func() {
			toml := `[database]
			name = "testdb"
			type = "test"`
			ap := &types.AcceptedProposal{Topic: "database", Data: []byte(toml)}

			Expect(len(c.Data.Databases)).To(Equal(0))
			err := c.route(ap)
			Expect(err).To(BeNil())
			Expect(len(c.Data.Databases)).To(Equal(1))
			Expect(c.Data.Databases).To(HaveKey("testdb"))
		})

		It("should correctly add a Syncable", func() {
			c.Data.Topics["test1"] = fakeTopic

			toml := `[syncable]
			name="foo"
			dbType = "test"
			[test]
			topic = "test1"`
			ap := &types.AcceptedProposal{Topic: "syncable", Data: []byte(toml)}

			Expect(len(c.Data.Syncables)).To(Equal(0))
			err := c.route(ap)
			Expect(err).To(BeNil())
			Expect(len(c.Data.Syncables)).To(Equal(1))
			Expect(c.Data.Syncables).To(HaveKey("foo"))
		})

		It("should append data when not using a reserved topic name", func() {
			c.Data.Topics["test1"] = fakeTopic

			ap := &types.AcceptedProposal{Topic: "test1", Index: 1, Term: 2, Data: []byte("bar")}
			err := c.route(ap)
			Expect(err).To(BeNil())
			Expect(fakeTopic.AppendCallCount()).To(Equal(1))
			topicData := topic.Data{Index: ap.Index, Term: ap.Term, Data: ap.Data}
			Expect(fakeTopic.AppendArgsForCall(0)).To(Equal(topicData))
		})

		It("should error when appending to a topic that doesn't exist", func() {
			ap := &types.AcceptedProposal{Topic: "bogusTopic"}
			err := c.route(ap)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("Attempting to append to topic"))
		})
	})
})
