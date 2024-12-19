package db

import (
	"context"
	"fmt"
	"io"

	"github.com/philborlin/committed/internal/cluster"
)

func (db *DB) Sync(ctx context.Context, id string, s cluster.Syncable) error {
	// State Machine
	// leader -> leader - keep syncing
	// leader -> not-leader - stop syncing
	// not-leader -> not-leader - keep not-syncing
	// not-leader -> leader - start syncing

	go func() {
		_ = db.sync(ctx, id, s)
	}()

	return nil
}

func (db *DB) sync(ctx context.Context, id string, s cluster.Syncable) error {
	isNode := false

	var r ProposalReader

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if isNode && !db.isNode(id) {
				fmt.Println("Stopping Sync...")
				// leader -> not-leader - stop syncing
				r = nil
				isNode = false
			} else if !isNode && db.isNode(id) {
				fmt.Printf("[db] Syncing %v\n", id)
				// not-leader -> leader - start syncing
				r = db.storage.Reader(id)
				isNode = true
			} else if isNode && db.isNode(id) {
				// leader -> leader - keep syncing
				i, p, err := r.Read()
				if err == io.EOF {
					// TODO Figure out what to do - maybe do an exponential backoff to a certain point - maybe nothing?
					continue
				} else if err != nil {
					// TODO Handle error
					fmt.Printf("[db.DB] read: %v\n", err)
				}

				shouldSnapshot, err := s.Sync(db.ctx, p)
				if err != nil {
					// TODO Handle error
					fmt.Printf("[db.DB] sync: %v\n", err)
				}

				if shouldSnapshot {
					err = db.proposeSyncableIndex(&cluster.SyncableIndex{ID: id, Index: i})
					if err != nil {
						// TODO Handle error
						fmt.Printf("[db.DB] proposeSyncableIndex: %v\n", err)
					}
				}
			}
		}
	}
}
