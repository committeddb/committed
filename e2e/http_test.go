package e2e

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

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
		// control.shutdown(false)
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

	It("loads topic on reload (applies snapshot)", func() {
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

	It("Catches up after dropping from the quorum", func() {
		err = control.postTopic(topicTOML)
		Expect(err).To(BeNil())

		topic := "test1"
		syncableTOML := fileTOML("foo", topic, "./data/foo.txt")
		fmt.Println(syncableTOML)
		err = control.postSyncable(syncableTOML)
		Expect(err).To(BeNil())

		// time.Sleep(5 * time.Second)
		fmt.Println("[http_test.go] Shutdown node")

		err = control.shutdownNode()
		Expect(err).To(BeNil())

		time.Sleep(5 * time.Second)

		fmt.Println("[http_test.go] Posting")

		data := "bar"
		err := control.postPost(topic, data)
		Expect(err).To(BeNil())

		fmt.Println("[http_test.go] Waiting 5 seconds")

		time.Sleep(5 * time.Second)

		fmt.Println("[http_test.go] Done waiting, attempting to startup node")

		err = control.startupNode(1)
		if err != nil {
			fmt.Println(err)
		}
		Expect(err).To(BeNil())

		time.Sleep(5 * time.Second)

		// Pages do not get flushed until they are full. This forces a page flush on the wal
		err = control.shutdownNode()
		Expect(err).To(BeNil())

		time.Sleep(10 * time.Second)

		err = findStringInTopic(topic, 3, data)
		Expect(err).To(BeNil())

		err = findStringInTopic(topic, 2, data)
		Expect(err).To(BeNil())

		err = findStringInTopic(topic, 1, data)
		Expect(err).To(BeNil())

		printDir("./data")
		err = printFile("./data/foo.txt")
		Expect(err).To(BeNil())
	})
})

func fileTOML(name string, topic string, path string) string {
	return fmt.Sprintf("[syncable]\nname=\"%s\"\ndbType=\"file\"\n\n[file]\ntopic=\"%s\"\npath=\"%s\"",
		name, topic, path)
}

func printDir(name string) error {
	fmt.Printf("Dir: %v\n", name)
	files, err := ioutil.ReadDir(name)
	if err != nil {
		return err
	}

	for _, file := range files {
		fmt.Printf("  %v\n", file.Name())
	}

	return nil
}

func printFile(name string) error {
	s, err := getFileContents(name)
	if err != nil {
		return err
	}
	log.Printf("FileSyncable\n: %s\n", s)
	return nil
}

func getFileContents(name string) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}

	br := bufio.NewReader(file)
	var buf bytes.Buffer

	for {
		ba, isPrefix, err := br.ReadLine()

		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}

		buf.Write(ba)
		if !isPrefix {
			buf.WriteByte('\n')
		}

	}
	return buf.String(), nil
}
