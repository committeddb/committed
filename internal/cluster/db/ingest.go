package db

import (
	"context"
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
)

func (db *DB) Ingest(ctx context.Context, id string, i cluster.Ingestable) error {
	// State Machine
	// leader -> leader - keep ingesting
	// leader -> not-leader - stop ingesting
	// not-leader -> not-leader - keep not-ingesting
	// not-leader -> leader - start ingesting

	go func() {
		_ = db.ingest(ctx, id, i)
	}()

	return nil
}

func (db *DB) ingest(ctx context.Context, id string, i cluster.Ingestable) error {
	isNode := false

	proposalChan := make(chan *cluster.Proposal)
	positionChan := make(chan cluster.Position)
	var cancel context.CancelFunc

	for {
		select {
		case <-ctx.Done():
			return nil
		case proposal := <-proposalChan:
			if db.isNode(id) {
				err := db.Propose(proposal)
				if err != nil {
					// TODO Handle error
					fmt.Printf("[db.DB] propose: %v\n", err)
				}
			}
		case position := <-positionChan:
			if db.isNode(id) {
				err := db.proposeIngestablePosition(&cluster.IngestablePosition{ID: id, Position: position})
				if err != nil {
					// TODO Handle error
					fmt.Printf("[db.DB] proposeIngestablePosition: %v\n", err)
				}
			}
		default:
			if isNode && !db.isNode(id) {
				fmt.Println("Stopping ingestion...")
				// leader -> not-leader - stop ingesting
				isNode = false
				cancel()
			} else if !isNode && db.isNode(id) {
				fmt.Println("Starting to ingest...")
				// not-leader -> leader - start ingesting
				isNode = true

				var ingressCtx context.Context
				ingressCtx, cancel = context.WithCancel(context.Background())
				defer cancel()

				p := db.storage.Position(id)
				go func() {
					err := i.Ingest(ingressCtx, p, proposalChan, positionChan)
					if err != nil {
						// TODO Handle error
						fmt.Printf("[db.DB] ingest: %v\n", err)
					}
				}()
			}
		}
	}
}
