package topic

import (
	"context"
	"fmt"
	"io"

	"github.com/philborlin/committed/types"
	"github.com/philborlin/committed/types/typesfakes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Topic", func() {
	const (
		fakeName    = "foo"
		fakeBaseDir = "bar"
	)

	var (
		fakeData       Data
		fakeWal        *typesfakes.FakeWAL
		fakeWALFactory *typesfakes.FakeWALFactory
		fakeError      error  = fmt.Errorf("")
		fakeWalDir     string = fmt.Sprintf("%s/%s", fakeBaseDir, fakeName)
	)

	JustBeforeEach(func() {
		fakeData = Data{Index: 0, Term: 0, Data: []byte("baz")}
		fakeWal = &typesfakes.FakeWAL{}
		fakeWALFactory = &typesfakes.FakeWALFactory{}
		walFactory = fakeWALFactory
	})

	JustAfterEach(func() {
		walFactory = &types.TSDBWALFactory{}
	})

	Describe("New()", func() {
		It("should return a new topic", func() {
			fakeWALFactory.NewSizeReturns(fakeWal, nil)
			topic, err := New(fakeName, fakeBaseDir)
			Expect(err).To(BeNil())
			Expect(topic.Name()).To(Equal(fakeName))
			Expect(topic.walDir).To(Equal(fakeWalDir))
			Expect(topic.wal).To(Equal(fakeWal))
			Expect(fakeWALFactory.NewSizeCallCount()).To(Equal(1))
		})

		It("should err if walFactory errors", func() {
			fakeWALFactory.NewSizeReturns(nil, fakeError)
			topic, err := New(fakeName, fakeBaseDir)
			Expect(err).ToNot(BeNil())
			Expect(topic).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("Could not create wal for topic"))
		})
	})

	Describe("Restore()", func() {
		It("should restore an existing topic", func() {
			fakeWALFactory.OpenReturns(fakeWal, nil)
			topic, err := Restore(fakeName, fakeWalDir)
			Expect(err).To(BeNil())
			Expect(topic.Name()).To(Equal(fakeName))
			Expect(topic.walDir).To(Equal(fakeWalDir))
			Expect(topic.wal).To(Equal(fakeWal))
			Expect(fakeWALFactory.OpenCallCount()).To(Equal(1))
		})

		It("should err if walFactory errors", func() {
			fakeWALFactory.OpenReturns(nil, fakeError)
			topic, err := Restore(fakeName, fakeWalDir)
			Expect(err).ToNot(BeNil())
			Expect(topic).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("Could no open wal for topic"))
		})
	})

	Describe("Close()", func() {
		var topic *WALTopic

		JustBeforeEach(func() {
			topic = &WALTopic{wal: fakeWal}
		})

		It("should return nil when successful", func() {
			Expect(topic.Close()).To(BeNil())
		})

		It("should return an error when unsuccessful", func() {
			fakeWal.CloseReturns(fakeError)
			Expect(topic.Close()).To(Equal(fakeError))
		})
	})

	Describe("Append()", func() {
		var topic *WALTopic

		JustBeforeEach(func() {
			topic = &WALTopic{wal: fakeWal}
		})

		It("should return nil when successful", func() {
			Expect(topic.Append(fakeData)).To(BeNil())
		})

		It("should return an error when log fails", func() {
			fakeWal.LogReturns(fakeError)
			err := topic.Append(fakeData)
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("Error appending to wal for topic"))
		})

		It("should return nil when waiting is false", func() {
			alert := &topicReaderAlert{}
			topic.addReaderAlert(alert)
			Expect(topic.Append(fakeData)).To(BeNil())
		})

		It("should sent true to appendC when waiting is true", func(done Done) {
			c := make(chan bool)
			alert := &topicReaderAlert{waiting: true, appendC: c}
			topic.addReaderAlert(alert)

			go func() {
				_ = topic.Append(fakeData)
			}()

			Expect(<-c).To(Equal(true))

			close(done)
		}, 0.2)
	})

	Describe("NewReader()", func() {
		var (
			m     map[int]uint64
			topic *WALTopic
		)

		JustBeforeEach(func() {
			m = make(map[int]uint64)
			topic = &WALTopic{wal: fakeWal, firstIndexInSegment: m}
		})

		It("should return a reader when successful", func() {
			fakeWal.SegmentsReturns(0, 0, nil)
			fakeWALFactory.NewSegmentsRangeReaderReturns(nil, nil)

			r, err := topic.NewReader(0)
			Expect(err).To(BeNil())
			Expect(r).ToNot(BeNil())
		})

		It("should finds the segment with the highest index that doesn't go over index", func() {
			fakeWal.SegmentsReturns(0, 3, nil)
			m[0] = 0
			m[1] = 4
			m[2] = 8

			r, err := topic.NewReader(5)
			Expect(err).To(BeNil())
			Expect(r).ToNot(BeNil())
			wtr := r.(*walTopicReader)
			Expect(wtr.currentSegment).To(Equal(1))

			sr := fakeWALFactory.NewSegmentsRangeReaderArgsForCall(0)
			Expect(len(sr)).To(Equal(1))
			Expect(sr[0].First).To(Equal(1))
			Expect(sr[0].Last).To(Equal(1))
			// Expect(sr[0].Last).To(Equal(2))
		})

		It("should return an error when Segments() returns an erroru", func() {
			fakeWal.SegmentsReturns(0, 0, fakeError)

			r, err := topic.NewReader(0)
			Expect(err).ToNot(BeNil())
			Expect(r).To(BeNil())
		})

		It("should return an error when NewSegmentsRangeReader returns an error", func() {
			fakeWal.SegmentsReturns(0, 0, nil)
			fakeWALFactory.NewSegmentsRangeReaderReturns(nil, fakeError)

			r, err := topic.NewReader(0)
			Expect(err).ToNot(BeNil())
			Expect(r).To(BeNil())
		})
	})

	Describe("Next()", func() {
		var (
			err            error
			m              map[int]uint64
			topic          *WALTopic
			reader         Reader
			fakeLiveReader *typesfakes.FakeLiveReader
			ap1            *types.AcceptedProposal
			ap1Bytes       []byte
		)

		JustBeforeEach(func() {
			fakeLiveReader = &typesfakes.FakeLiveReader{}
			fakeWALFactory.NewLiveReaderReturns(fakeLiveReader)

			m = make(map[int]uint64)
			topic = &WALTopic{wal: fakeWal, firstIndexInSegment: m}
			reader, err = topic.NewReader(0)
			Expect(err).To(BeNil())

			ap1 = &types.AcceptedProposal{Topic: "foo", Index: 2, Term: 2, Data: []byte("bar")}
			ap1Bytes, err = ap1.Encode()
			Expect(err).To(BeNil())
		})

		It("should return an AcceptedProposal", func() {
			fakeLiveReader.RecordReturns(ap1Bytes)
			fakeLiveReader.NextReturns(true)

			proposal, err := reader.Next(context.TODO())
			Expect(proposal).To(Equal(ap1))
			Expect(err).To(BeNil())
			Expect(fakeLiveReader.RecordCallCount()).To(Equal(1))
			Expect(fakeLiveReader.NextCallCount()).To(Equal(1))
		})

		It("should move on to the next one if there is a corrupted proposal", func() {
			fakeLiveReader.RecordReturnsOnCall(0, nil)
			fakeLiveReader.RecordReturnsOnCall(1, ap1Bytes)
			fakeLiveReader.NextReturns(true)

			proposal, err := reader.Next(context.TODO())
			Expect(proposal).To(Equal(ap1))
			Expect(err).To(BeNil())
			Expect(fakeLiveReader.RecordCallCount()).To(Equal(2))
			Expect(fakeLiveReader.NextCallCount()).To(Equal(2))
		})

		It("should update FirstIndexInSegment if it is a new segment", func() {
			fakeWal.SegmentsReturns(0, 1, nil)
			fakeLiveReader.RecordReturns(ap1Bytes)
			fakeLiveReader.NextReturnsOnCall(0, false)
			fakeLiveReader.NextReturnsOnCall(1, true)
			fakeLiveReader.ErrReturns(io.EOF)

			proposal, err := reader.Next(context.TODO())
			Expect(proposal).To(Equal(ap1))
			Expect(err).To(BeNil())
			Expect(fakeLiveReader.RecordCallCount()).To(Equal(1))
			Expect(fakeLiveReader.NextCallCount()).To(Equal(2))
			Expect(fakeLiveReader.ErrCallCount()).To(Equal(1))
			Expect(fakeWal.SegmentsCallCount()).To(Equal(3))
			Expect(topic.firstIndexInSegment[1]).To(Equal(ap1.Index))
		})

		It("should return error if Segments within next() returns an error", func() {
			fakeWal.SegmentsReturns(0, 0, fakeError)
			fakeLiveReader.NextReturns(false)
			fakeLiveReader.ErrReturns(io.EOF)

			proposal, err := reader.Next(context.TODO())
			Expect(proposal).To(BeNil())
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("next() could not get the number of segments"))
		})

		It("should return error if Segments within newLiveReader() returns an error", func() {
			fakeWal.SegmentsReturnsOnCall(1, 0, 1, nil)
			fakeWal.SegmentsReturnsOnCall(2, 0, 0, fakeError)
			fakeLiveReader.NextReturns(false)
			fakeLiveReader.ErrReturns(io.EOF)

			proposal, err := reader.Next(context.TODO())
			Expect(proposal).To(BeNil())
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("newLiveReader() could not get the number of segments"))
		})

		It("should wait if there is an EOF without an error", func(done Done) {
			fakeLiveReader.NextReturnsOnCall(0, false)
			fakeLiveReader.NextReturnsOnCall(1, true)
			fakeLiveReader.RecordReturns(ap1Bytes)
			fakeLiveReader.ErrReturns(io.EOF)
			fakeWal.SegmentsReturns(0, 0, nil)

			walTopicReader := reader.(*walTopicReader)
			alert := walTopicReader.alert

			go func() {
				alert.appendC <- true
			}()

			proposal, err := reader.Next(context.TODO())
			Expect(proposal).To(Equal(ap1))
			Expect(err).To(BeNil())

			close(done)
		}, 2)

		It("should return error if the wal is corrupt", func() {
			fakeLiveReader.NextReturns(false)
			fakeLiveReader.ErrReturns(fakeError)

			proposal, err := reader.Next(context.TODO())
			Expect(proposal).To(BeNil())
			Expect(err).ToNot(BeNil())
			Expect(err.Error()).To(ContainSubstring("the wal is corrupt"))
		})
	})
})
