package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/interpretation"
)

// restatementProbeVersion is a sentinel rebind target used only inside the
// dry-run's match probe: a registry holding just the candidate (rebound to
// this impossible version) answers "did the restatement select this entity"
// exactly — range, stamp, AND predicate — through the same fold the read
// path runs. Never resolved against the type registry, never admitted.
const restatementProbeVersion = -1 << 30

// restatementDryRunMaxSamples caps the before/after sample list.
const restatementDryRunMaxSamples = 10

// DryRunRestatement rehearses a restatement against the committed log: full
// admission-level validation (same checks, same words as ProposeRestatement —
// minus the append-only id check, since nothing is being admitted), then a
// scan of the restatement's own [fromIndex, toIndex] range reporting what it
// selects and what it changes. Never proposes, never writes — a node-local
// diagnostic read. See cluster.RestatementDryRunReport.
func (db *DB) DryRunRestatement(ctx context.Context, mimeType string, data []byte, opts cluster.DryRunOptions) (*cluster.RestatementDryRunReport, error) {
	start := time.Now()
	e, err := ParseRestatement(&cluster.Configuration{ID: "dryrun", MimeType: mimeType, Data: data})
	if err != nil {
		return nil, cluster.NewConfigError(err)
	}

	// The SAME storage-backed admission checks the real POST runs — shared
	// code, not a mirror, so the rehearsal cannot drift from the refusals.
	if err := db.admitRestatementChecks(e); err != nil {
		return nil, err
	}
	parseMs := time.Since(start).Milliseconds()

	// Three PRIVATE registries, compiled fresh from the applied records: the
	// current fold, the candidate fold (current + this restatement at a
	// past-everything coordinate), and the match probe (the candidate alone,
	// rebound to a sentinel so "selected" is directly observable). Never the
	// live shared snapshot: its predicate ambiguity trackers pool evidence
	// for the real workers, and a rehearsal must not feed or reset them.
	appliedRestatements, err := db.storage.AppliedRestatements()
	if err != nil {
		return nil, err
	}
	current, err := interpretation.NewRegistry(appliedRestatements)
	if err != nil {
		return nil, err
	}
	coord := db.storage.AppliedIndex() + 1
	candidate, err := interpretation.NewRegistry(append(slices.Clone(appliedRestatements), cluster.AppliedRestatement{Restatement: *e, Index: coord}))
	if err != nil {
		return nil, err
	}
	pe := *e
	pe.ReadAsVersion = restatementProbeVersion
	probe, err := interpretation.NewRegistry([]cluster.AppliedRestatement{{Restatement: pe, Index: coord}})
	if err != nil {
		return nil, err
	}

	rep := &cluster.RestatementDryRunReport{
		ScanFrom:         e.FromIndex,
		ScanTo:           e.ToIndex,
		ByStampedVersion: map[int]int{},
		Findings:         []string{},
	}

	budget := opts.MaxEntries
	if budget <= 0 {
		budget = dryRunDefaultEntries
	}

	var readMs int64
	var firstPredicateErr string
	coverageComplete := false
	// ReaderAt uses checkpoint semantics (the index is already-consumed, the
	// first Read returns the entry AFTER it); the restatement's range is
	// inclusive, so start one below. ParseRestatement guarantees FromIndex >= 1.
	r := db.storage.ReaderAt(e.FromIndex - 1)
scan:
	for rep.EntriesScanned < budget {
		if ctx.Err() != nil {
			rep.Truncated = "deadline reached during the scan — counters are partial; raise ?timeoutSeconds or lower ?maxEntries"
			break
		}
		readStart := time.Now()
		a, err := r.Read()
		readMs += time.Since(readStart).Milliseconds()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// The reader ran past the log head; toIndex <= applied, so
				// the whole range was read.
				coverageComplete = true
				break
			}
			return nil, fmt.Errorf("log read failed at the dry-run scan: %w", err)
		}
		if a.Index > e.ToIndex {
			coverageComplete = true
			break
		}
		rep.EntriesScanned++
		for _, ent := range a.Entities {
			if ent.Type == nil || cluster.IsInternal(ent.ID) || ent.Variant() != cluster.EntityVariantRow {
				continue
			}
			if ent.ID != e.TypeID {
				continue
			}
			rep.EntitiesOfType++
			if !e.Matches(a.Index, ent.Version) {
				continue
			}
			rep.StampEligible++
			rep.ByStampedVersion[ent.Version]++

			// Did the restatement SELECT this entity (predicate included)?
			probeEff, perr := probe.EffectiveVersion(ctx, ent.ID, a.Index, ent.Version, ent.Data)
			if perr != nil {
				rep.PredicateErrors++
				if firstPredicateErr == "" {
					firstPredicateErr = perr.Error()
				}
				continue
			}
			if probeEff != restatementProbeVersion {
				continue // predicate filtered it out
			}
			rep.Matched++

			// Does its READING change — the restatement's real effect?
			curEff, cerr := current.EffectiveVersion(ctx, ent.ID, a.Index, ent.Version, ent.Data)
			candEff, derr := candidate.EffectiveVersion(ctx, ent.ID, a.Index, ent.Version, ent.Data)
			if cerr != nil || derr != nil {
				// An already-applied restatement's predicate failed on this row —
				// the live path would dead-letter it; count, don't abort.
				rep.PredicateErrors++
				if firstPredicateErr == "" {
					if cerr == nil {
						cerr = derr
					}
					firstPredicateErr = cerr.Error()
				}
				continue
			}
			if candEff == curEff {
				continue // a no-op: the target is already this entity's reading
			}
			rep.Rebound++
			if len(rep.Samples) < restatementDryRunMaxSamples {
				rep.Samples = append(rep.Samples, cluster.RestatementDryRunSample{
					Index:            a.Index,
					Key:              string(ent.Key),
					StampedVersion:   ent.Version,
					CurrentReading:   curEff,
					CandidateReading: candEff,
				})
			}
		}
		select {
		case <-ctx.Done():
			rep.Truncated = "deadline reached during the scan — counters are partial; raise ?timeoutSeconds or lower ?maxEntries"
			break scan
		default:
		}
	}
	if !coverageComplete && rep.Truncated == "" {
		rep.Truncated = fmt.Sprintf("entry budget (%d) reached before toIndex — counters cover [%d, budget end]; raise ?maxEntries", budget, e.FromIndex)
	}
	rep.Coverage = "partial"
	if coverageComplete {
		rep.Coverage = "complete"
	}
	rep.ReadMs = readMs
	rep.ParseMs = parseMs

	db.restatementDryRunAdvisories(e, rep, firstPredicateErr)
	rep.DurationMs = time.Since(start).Milliseconds()
	return rep, nil
}

// restatementDryRunAdvisories writes the staleness preview, the overlap notes,
// and the auto-flagged authoring signatures.
func (db *DB) restatementDryRunAdvisories(e *cluster.Restatement, rep *cluster.RestatementDryRunReport, firstPredicateErr string) {
	// Which syncables consume this topic — the re-materialization bill.
	if configs, err := db.storage.Syncables(); err == nil {
		for _, cfg := range configs {
			topics, terr := db.parser.SyncableTopics(cfg.MimeType, cfg.Data)
			if terr != nil || !slices.Contains(topics, e.TypeID) {
				continue
			}
			pin, stale, serr := db.SyncableInterpretation(cfg.ID)
			if serr != nil {
				continue
			}
			rep.AffectedSyncables = append(rep.AffectedSyncables, cluster.RestatementDryRunSyncable{
				ID: cfg.ID, InterpretationPin: pin, AlreadyStale: stale,
			})
		}
	}

	// Already-applied restatements this one composes with (later in the log wins).
	if applied, err := db.storage.AppliedRestatements(); err == nil {
		for _, a := range applied {
			if a.Restatement.TypeID == e.TypeID && a.Restatement.FromIndex <= e.ToIndex && e.FromIndex <= a.Restatement.ToIndex {
				rep.Overlaps = append(rep.Overlaps, a.Restatement.ID)
			}
		}
	}

	partial := rep.Coverage != "complete"
	switch {
	case rep.Matched == 0 && !partial:
		rep.Findings = append(rep.Findings, "the restatement matches NOTHING in its range: no entity passes the range + stamp + predicate selectors — check the index range, fromVersion, and predicate")
	case rep.Matched == 0 && partial:
		rep.Findings = append(rep.Findings, "no match in the scanned part of the range (coverage partial) — raise ?maxEntries before concluding the selectors are wrong")
	case rep.Rebound == 0:
		rep.Findings = append(rep.Findings, fmt.Sprintf("the restatement matches %d entities but changes NO readings: every match already reads as version %d — admitting it would only stale consumers for a no-op", rep.Matched, e.ReadAsVersion))
	}
	if e.Predicate != "" && rep.StampEligible > 0 && rep.Matched == rep.StampEligible {
		rep.Findings = append(rep.Findings, "the predicate matched every stamp-eligible entity in the scan — verify it is actually narrowing (a jq program that maps everything to true selects the whole range)")
	}
	if rep.PredicateErrors > 0 {
		rep.Findings = append(rep.Findings, fmt.Sprintf("%d entities failed predicate evaluation (first: %s) — on the live path each of these dead-letters or wedges a consumer", rep.PredicateErrors, firstPredicateErr))
	}
	if n := len(rep.AffectedSyncables); n > 0 {
		rep.Findings = append(rep.Findings, fmt.Sprintf("admitting this restatement marks %d syncable(s) stale: their materialized rows keep the superseded reading until re-materialized (POST /v1/syncable/{id}/rematerialize)", n))
	}
	if !db.featureEnabled(featureLevelRestatements) {
		rep.Findings = append(rep.Findings, fmt.Sprintf("the real POST would currently be refused: the cluster minimum feature level is %d, restatements require %d — finish the rolling upgrade first", db.clusterMinFeatureLevel(), featureLevelRestatements))
	}
}
