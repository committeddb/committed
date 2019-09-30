package e2e

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/philborlin/committed/topic"
)

func findStringInTopic(topicName string, node int, s string) error {
	t, err := topic.Restore(topicName, fmt.Sprintf("./Data/raft-%d/%s", node, topicName))
	if err != nil {
		return err
	}

	r, err := t.NewReader(0)
	if err != nil {
		return err
	}

	for {
		p, err := r.Next(context.Background())
		if err != nil {
			return err
		}

		b := p.Data
		log.Printf("[%d] Looking for [%s] in [%s]", node, s, string(b))
		if strings.Contains(string(b), s) {
			return nil
		}
	}
}

// func getWalReader(topic string, node int) (types.LiveReader, error) {
// 	factory := &types.TSDBWALFactory{}

// 	sr := wal.SegmentRange{Dir: fmt.Sprintf("./Data/raft-%d/%s", node, topic), First: 0, Last: -1}
// 	srr, err := factory.NewSegmentsRangeReader(sr)
// 	if err != nil {
// 		return nil, err
// 	}

// 	lr := factory.NewLiveReader(nil, nil, srr)

// 	return lr, nil
// }

// func findStringInReader(r types.LiveReader, s string, id int) error {
// 	for r.Next() {
// 		b := r.Record()
// 		log.Printf("[%d] Looking for [%s] in [%s]", id, s, string(b))
// 		if strings.Contains(string(b), s) {
// 			return nil
// 		}
// 	}

// 	return fmt.Errorf("string [%s] did not exist in wal", s)
// }

// func findStringInTopicReader(topic string, s string, node int) error {
// 	r, err := getWalReader(topic, node)
// 	if err != nil {
// 		return err
// 	}
// 	return findStringInReader(r, s, node)
// }
