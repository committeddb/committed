package db

import (
	"context"
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
)

func (db *DB) Ingest(ctx context.Context, id string, i cluster.Ingestable) error {
	// TODO If transition to leader, start ingesting, if transition off leader, stop ingesting

	// State Machine
	// leader -> leader - keep ingesting
	// leader -> not-leader - stop ingesting
	// not-leader -> not-leader - keep not-ingesting
	// not-leader -> leader - start ingesting

	// timer that checks state on each tick - we alread have this from the raft algorithm
	// can we message a channel every tick and check for state changes?

	// We already know how to ingest, now we need to turn it on/off based on state changes and then drive the state changes

	go func() {
		_ = db.ingest(ctx, id, i)
	}()

	return nil
}

func (db *DB) ingest(ctx context.Context, id string, i cluster.Ingestable) error {
	isLeader := false

	// ls := NewLeaderState(true)
	ls := db.leaderState

	fmt.Printf("Is leader? %v\n", db.leaderState.IsLeader())

	proposalChan := make(chan *cluster.Proposal)
	positionChan := make(chan cluster.Position)
	var cancel context.CancelFunc

	for {
		select {
		case <-ctx.Done():
			return nil
		case proposal := <-proposalChan:
			// if ls.IsLeader() {
			err := db.Propose(proposal)
			if err != nil {
				// TODO Handle error
				fmt.Printf("[db.DB] propose: %v\n", err)
			}
			// }
		case position := <-positionChan:
			// if ls.IsLeader() {
			err := db.proposeIngestablePosition(&cluster.IngestablePosition{ID: id, Position: position})
			if err != nil {
				// TODO Handle error
				fmt.Printf("[db.DB] proposeIngestablePosition: %v\n", err)
			}
			// }
		default:
			if isLeader && !ls.IsLeader() {
				fmt.Println("Stopping ingestion...")
				// leader -> not-leader - stop ingesting
				isLeader = false
				cancel()
			} else if !isLeader && ls.IsLeader() {
				fmt.Println("Starting to ingest...")
				// not-leader -> leader - start ingesting
				isLeader = true

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
