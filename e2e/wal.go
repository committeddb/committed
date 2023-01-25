package e2e

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/philborlin/committed/internal/node/topic"
	"github.com/pkg/errors"
)

func findStringInTopic(topicName string, node int, s string) error {
	t, err := topic.Restore(topicName, fmt.Sprintf("./data/raft-%d/%s", node, topicName))
	if err != nil {
		return errors.Wrapf(err, "Error restoring %s", fmt.Sprintf("./data/raft-%d/%s", node, topicName))
	}

	r, err := t.NewReader(0)
	if err != nil {
		return errors.Wrap(err, "Error creating NewReader")
	}

	for {
		p, err := r.Next(context.Background())
		if err != nil {
			return errors.Wrap(err, "Error getting next")
		}

		b := p.Data
		log.Printf("[%d] Looking for [%s] in [%s]", node, s, string(b))
		if strings.Contains(string(b), s) {
			return nil
		}
	}
}
