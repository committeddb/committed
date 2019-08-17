package bridge

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/syncable/syncablefakes"
	"github.com/philborlin/committed/topic"
	"github.com/philborlin/committed/topic/topicfakes"
	"github.com/philborlin/committed/types"
)

var _ = Describe("Topic", func() {
	const (
		bridgeName string = "qux"
	)

	var (
		fooFakeTopic *topicfakes.FakeTopic
		barFakeTopic *topicfakes.FakeTopic
		fakeReader   *topicfakes.FakeReader

		topics       map[string]topic.Topic
		fakeSyncable *syncablefakes.FakeSyncable
		bridgeFactory Factory
	)

	JustBeforeEach(func() {
		fooFakeTopic = &topicfakes.FakeTopic{}
		fooFakeTopic.NameReturns("foo")
		barFakeTopic = &topicfakes.FakeTopic{}
		barFakeTopic.NameReturns("bar")

		fakeReader = &topicfakes.FakeReader{}
		fooFakeTopic.NewReaderReturns(fakeReader, nil)

		topics = map[string]topic.Topic{"foo": fooFakeTopic, "bar": barFakeTopic}
		fakeSyncable = &syncablefakes.FakeSyncable{}
		bridgeFactory = &TopicSyncableBridgeFactory{}
	})

	new := func(name string, s syncable.Syncable, topics map[string]topic.Topic) (*TopicSyncableBridge, error) {
		b, err := bridgeFactory.New(name, fakeSyncable, topics)
		if err != nil {
			return nil, err
		}
		return b.(*TopicSyncableBridge), err
	}

	Describe("NewBridge()", func() {
		It("should create a new bridge", func() {
			fakeSyncable.TopicsReturns([]string{"foo"})
			b, err := new(bridgeName, fakeSyncable, topics)
			Expect(err).To(BeNil())
			Expect(len(b.topics)).To(Equal(1))
			Expect(b.topics["foo"]).To(Equal(fooFakeTopic))
			Expect(b.Name).To(Equal(bridgeName))
			Expect(b.Syncable).To(Equal(fakeSyncable))
			Expect(b.lastIndex).To(Equal(uint64(0)))
		})

		It("should error if there are no topics", func() {
			fakeSyncable.TopicsReturns([]string{})
			b, err := new(bridgeName, fakeSyncable, topics)
			Expect(b).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("No topics so there is nothing to sync"))
		})

		It("should error if there are too many topics", func() {
			fakeSyncable.TopicsReturns([]string{"foo", "bar"})
			b, err := new(bridgeName, fakeSyncable, topics)
			Expect(b).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("We don't support more than one topic in a syncable yet"))
		})

		It("should error if topic does not exist", func() {
			fakeSyncable.TopicsReturns([]string{"baz"})
			b, err := new(bridgeName, fakeSyncable, topics)
			Expect(b).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("is trying to listen to topic"))
		})
	})

	Describe("GetSnapshot()/ApplySnapshot()", func() {
		var (
			b   *TopicSyncableBridge
			err error
		)

		JustBeforeEach(func() {
			fakeSyncable.TopicsReturns([]string{"foo"})
			b, err = new(bridgeName, fakeSyncable, topics)
			Expect(err).To(BeNil())
		})

		It("should create and restore a snapshot", func() {
			b.lastIndex = 1
			s, err := b.GetSnapshot()
			Expect(err).To(BeNil())

			b.lastIndex = 0
			err = b.ApplySnapshot(s)
			Expect(err).To(BeNil())
			Expect(b.lastIndex).To(Equal(uint64(1)))
		})

		It("should error if snapshot is corrupt", func() {
			err := b.ApplySnapshot([]byte{})
			Expect(err.Error()).To(ContainSubstring("Could not decode snapshot"))
		})
	})

	Describe("Init()", func() {
		var (
			errorC    chan error
			fakeError error
			err       error
			b         *TopicSyncableBridge
			ctx       context.Context
		)

		JustBeforeEach(func() {
			errorC = make(chan error)
			fakeError = fmt.Errorf("fake error")
			fakeSyncable.TopicsReturns([]string{"foo"})
			b, err = new(bridgeName, fakeSyncable, topics)
			Expect(err).To(BeNil())
			ctx = context.Background()
		})

		It("should error syncable init fails", func() {
			fakeSyncable.InitReturns(fakeError)
			err = b.Init(context.Background(), errorC)
			Expect(err.Error()).To(ContainSubstring("Init of internal syncable failed"))
		})

		It("should error if topic can't create a new reader", func() {
			fooFakeTopic.NewReaderReturns(nil, fakeError)
			err = b.Init(context.Background(), errorC)
			Expect(err.Error()).To(ContainSubstring("Could not create reader"))
		})

		// TODO Cancelling isn't ready yet
		XIt("should error if ctx is done and the syncable has a close error", func(done Done) {
			fakeSyncable.CloseReturns(fakeError)
			ctx, cancel := context.WithCancel(ctx)

			go func() {
				_ = b.Init(ctx, errorC)
			}()

			cancel()

			err = <-errorC
			Expect(err.Error()).To(ContainSubstring("Problem closing wrapped syncable"))

		}, 0.2)

		It("should err next call on reader has an error", func(done Done) {
			fakeReader.NextReturns(nil, fakeError)

			go func() {
				_ = b.Init(ctx, errorC)
			}()

			err = <-errorC
			Expect(err.Error()).To(ContainSubstring("Problem getting the next accepted proposal from topic"))
			Expect(fakeReader.NextCallCount()).To(Equal(2))

			close(done)
		}, 0.2)

		It("should err if the call to sync has an error", func(done Done) {
			fakeSyncable.SyncReturns(fakeError)

			go func() {
				_ = b.Init(ctx, errorC)
			}()

			err = <-errorC
			Expect(err.Error()).To(ContainSubstring("Problem syncing"))
			Expect(fakeReader.NextCallCount()).To(Equal(2))

			close(done)
		}, 0.2)

		It("should update lastIndex if a successful sync occurs", func(done Done) {
			ap := &types.AcceptedProposal{Index: 2}
			fakeReader.NextReturnsOnCall(1, ap, nil)
			fakeReader.NextReturns(nil, fakeError)

			Expect(b.lastIndex).To(Equal(uint64(0)))

			go func() {
				_ = b.Init(ctx, errorC)
			}()

			err = <-errorC
			Expect(b.lastIndex).To(Equal(uint64(2)))
			Expect(fakeReader.NextCallCount()).To(Equal(3))

			close(done)
		})
	})
})
