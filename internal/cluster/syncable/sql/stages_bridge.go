package sql

import (
	"math/big"

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
	StageEmit       = stages.Emit
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

func compileExpr(src string) (stages.Node, error) { return stages.Compile(src) }

func evalExpr(n stages.Node, payload, parent any) (any, error) {
	return stages.Eval(n, payload, parent)
}

func formatRat(r *big.Rat) (string, error) { return stages.FormatRat(r) }

func validateWhenClauses(c []WhenClause, where string) error { return stages.ValidateWhen(c, where) }

func stageNamed(c *ProjectionConfig, name string) int { return stages.IndexOf(c.Stages, name) }

func stageFingerprint(c *ProjectionConfig) string { return stages.Fingerprint(c.Stages) }

func decodeStageObject(bs []byte) (any, error) { return stages.DecodeObject(bs) }
