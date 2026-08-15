package db

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// The validation tripwire (ValidateAnnounce): commit divergent payloads, and
// announce the FIRST occurrence of each distinct divergent shape as a
// ContractExtension event on the type's SchemaChangeTopic. It runs in Propose
// — the seam direct proposals, CDC's ordered lane (proposeIngestData), and
// any future proposer cross — plus one explicit call in the ingest worker's
// pipelined snapshot lane, which submits via proposeAsync and would otherwise
// bypass it. It is a tripwire, NEVER a gate: no failure in here may block or
// fail the data proposal. The announce itself is a separate
// proposal, committed BEFORE the data: if a crash lands between the two, the
// data re-proposes (its position was not yet checkpointed) and the dedupe
// mark suppresses a duplicate announcement; the reverse order would let a
// crash swallow the only announcement of a shape whose rows are already
// committed and SourceSeq-deduped from ever re-validating.
//
// Concurrency note: two nodes detecting one shape concurrently can both
// announce (each read the mark as absent). That is the documented
// at-least-once contract — consumers key on (type, version, fingerprint) and
// keyed sinks converge.

// announceDivergences scans an outgoing proposal for announce-typed row
// entities, validates each, and announces new divergent shapes. Failures are
// logged, never returned — the caller proposes the data regardless.
func (db *DB) announceDivergences(ctx context.Context, p *cluster.Proposal) {
	if db.entityValidator == nil {
		return
	}
	// The emitter's own proposals carry a contract-fingerprint entity; never
	// re-inspect them (belt and braces — admission also refuses an
	// announce-typed destination, so an event entity can't be announce-typed).
	for _, e := range p.Entities {
		if e.Type != nil && cluster.IsContractFingerprint(e.Type.ID) {
			return
		}
	}

	// One shape can repeat across a batch's entities; announce it once.
	seen := map[string]bool{}
	for _, e := range p.Entities {
		t := e.Type
		if t == nil || t.Validate != cluster.ValidateAnnounce {
			continue
		}
		if e.Variant() != cluster.EntityVariantRow || len(e.Data) == 0 {
			continue // deletes and markers carry no payload to validate
		}

		div, err := db.entityValidator.ValidateEntityData(t, e.Data)
		if err != nil {
			// Structurally unusable schema or payload — not a divergence, and
			// never a reason to hold data. Loud so a broken announce setup is
			// visible.
			db.logger.Warn("tripwire: cannot validate entity payload; committing without announce",
				zap.String("type", t.ID), zap.Int("version", t.Version), zap.Error(err))
			continue
		}
		if div == nil {
			continue
		}

		shape, fingerprint, err := cluster.JSONShapeSignature(e.Data)
		if err != nil {
			db.logger.Warn("tripwire: cannot fingerprint divergent payload; committing without announce",
				zap.String("type", t.ID), zap.Error(err))
			continue
		}
		if seen[fingerprint] || db.storage.HasContractFingerprint(t.ID, t.Version, fingerprint) {
			continue // this shape has already been announced
		}

		if db.announceShape(ctx, t, p, shape, fingerprint, div) {
			seen[fingerprint] = true
		}
	}
}

// announceShape emits one ContractExtension event plus its dedupe mark as one
// atomic proposal. Returns whether the announcement committed (so the caller
// can suppress re-announcing the shape within this batch either way — a false
// here retries on the shape's NEXT occurrence).
func (db *DB) announceShape(ctx context.Context, t *cluster.Type, data *cluster.Proposal, shape []string, fingerprint string, div *cluster.SchemaDivergence) bool {
	destRef := cluster.LatestTypeRef(t.SchemaChangeTopic)
	dest, err := db.storage.ResolveType(destRef)
	if err != nil || dest == nil {
		// Admission guaranteed the destination existed at POST; it has since
		// been deleted. Loud floor: the divergence is committed but
		// unannounced until the operator repairs the destination.
		db.logger.Error("tripwire: schemaChangeTopic no longer resolves; divergence committed but NOT announced — re-create the events topic or re-POST the type",
			zap.String("type", t.ID), zap.String("schemaChangeTopic", t.SchemaChangeTopic), zap.Error(err))
		return false
	}

	event := &cluster.ContractExtension{
		TypeID:        t.ID,
		TypeName:      t.Name,
		Version:       t.Version,
		Fingerprint:   fingerprint,
		ObservedShape: shape,
		Violations:    div.Causes,
		IngestableID:  data.IngestableID,
		SourceSeq:     data.SourceSeq,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		db.logger.Error("tripwire: marshal ContractExtension", zap.String("type", t.ID), zap.Error(err))
		return false
	}
	mark, err := cluster.NewContractFingerprintEntity(&cluster.ContractFingerprint{
		TypeID: t.ID, Version: t.Version, Fingerprint: fingerprint,
	})
	if err != nil {
		db.logger.Error("tripwire: build fingerprint mark", zap.String("type", t.ID), zap.Error(err))
		return false
	}

	// Event first: proposalKind classifies by the first entity, so the pair
	// admits as user data (blocked at disk-critical exactly when data is).
	announce := &cluster.Proposal{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(dest, event.Key(), payload),
		mark,
	}}
	if err := db.Propose(ctx, announce); err != nil {
		// The mark didn't commit either, so the shape's next occurrence
		// retries — at-least-once, never silent-forever.
		db.logger.Error("tripwire: announce proposal failed; will retry on the shape's next occurrence",
			zap.String("type", t.ID), zap.String("schemaChangeTopic", t.SchemaChangeTopic),
			zap.String("fingerprint", fingerprint), zap.Error(err))
		return false
	}

	db.logger.Info("tripwire: contract divergence announced",
		zap.String("type", t.ID), zap.Int("version", t.Version),
		zap.String("schemaChangeTopic", t.SchemaChangeTopic),
		zap.String("fingerprint", fingerprint),
		zap.String("ingestable", data.IngestableID))
	return true
}
