package sql

import (
	"fmt"

	"github.com/committeddb/committed/internal/cluster/syncable/stages"
)

// The staged-computation engine lives in the sink-agnostic stages
// package (per the terminal rule: stages are middleware any syncable
// kind can host; this package wires them to its table terminal). The
// aliases keep this package's config surface stable, and the thin
// delegates keep its many internal call sites unchanged.

type (
	WhenClause      = stages.WhenClause
	ProjectionStage = stages.Stage
	StageMergeEntry = stages.MergeEntry
	StageEmit       = stages.Emit
	StageFanArm     = stages.FanArm
	StageJoin       = stages.Join
)

func matchWhen(c []WhenClause, d any) bool { return stages.Match(c, d) }

func isScalar(v any) bool { return stages.IsScalar(v) }

func keyString(v any) string { return stages.KeyString(v) }

func multiValuedPath(p string) bool { return stages.MultiValuedPath(p) }

func rejectMultiValued(p, where string) error { return stages.RejectMultiValued(p, where) }

func resolveScopedPath(path string, data, parent any) (any, error) {
	return stages.ResolvePath(path, data, parent)
}

// tableSourceWhy is why source() has no meaning at table level: a table
// rule reads one topic's (or stage's) payload, and source() names a
// stage input's from source.
const tableSourceWhy = "source() names a stage input's from source; a table rule reads its own source's payload — compute it in a stage (emit field = \"kind\", expr = \"source()\") and read the field"

func compileExpr(src string) (stages.Node, error) {
	n, err := stages.Compile(src)
	if err == nil && stages.UsesSource(n) {
		return nil, fmt.Errorf("%s", tableSourceWhy)
	}
	return n, err
}

func evalExpr(n stages.Node, payload, parent any) (any, error) {
	return stages.Eval(n, payload, parent)
}

func materializeExprValue(v any) (any, error) { return stages.Materialize(v) }

func validateWhenClauses(c []WhenClause, where string) error {
	if err := stages.ValidateWhen(c, where); err != nil {
		return err
	}
	return stages.RejectSourceInWhens(c, where, tableSourceWhy)
}

func stageNamed(c *ProjectionConfig, name string) int { return stages.IndexOf(c.Stages, name) }

func stageFingerprint(c *ProjectionConfig) string { return stages.Fingerprint(c.Stages) }

func decodeStageObject(bs []byte) (any, error) { return stages.DecodeObject(bs) }

func isBareOrderPath(s string) bool { return stages.IsBareOrderPath(s) }
